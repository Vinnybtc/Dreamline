#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Dreamline — live roast-dashboard voor Beans & Dreams.

Leest de Phidget 1048 (thermokoppels) rechtstreeks uit en stuurt alleen de
meetwaarden naar de iPad, die de curve zelf tekent. Geen schermspiegeling.

Gebruik (op de laptop):
    python dreamline.py            # echte chip; valt automatisch terug op simulatie
    python dreamline.py --sim      # forceer simulatie (geen hardware nodig)
    python dreamline.py --port 8080 --bt-ch 1 --et-ch 0 --tc K

Open daarna op de iPad in Safari het adres dat hieronder geprint wordt
(bijv. http://192.168.1.23:8080) en kies 'Zet op beginscherm'.
"""

import argparse, json, math, socket, threading, time, queue, webbrowser, sys
import sqlite3, ast, datetime, glob, os, shutil, urllib.request, urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

HERE = Path(__file__).resolve().parent
INDEX = HERE / "index.html"
DB_PATH = HERE / "roasts.db"
sys.path.insert(0, str(HERE))   # eigen modules (qr) vindbaar maken, ook met meegeleverde Python
try:
    import qr            # eigen QR-generator (geen externe dependency)
except Exception:
    qr = None

# --- versie & veilig updaten op afstand ------------------------------------
VERSION = "1.6.3"
CONFIG_PATH = HERE / "update_config.json"   # {"manifest_url": "https://.../manifest.json"}
BACKUP_DIR = HERE / "backup"                # vorige versie, voor 1-tik rollback
UPDATABLE = ("index.html", "dreamline.py", "qr.py")   # alleen deze mag een update vervangen
MAX_BODY = 8 * 1024 * 1024                  # max grootte van een POST (DoS-bescherming)
MAX_FILE = 6 * 1024 * 1024                  # max grootte van een te downloaden bestand

# --------------------------------------------------------------------------
# Gedeelde roast-toestand
# --------------------------------------------------------------------------
class State:
    def __init__(self):
        self.lock = threading.RLock()
        self.samples = []          # [{"t":sec_na_charge, "et":C, "bt":C}]
        self.events = {}           # type -> t (sec na charge)
        self.charge = None         # monotonic tijd van CHARGE
        self.bt = None
        self.et = None
        self.ror = None
        self.subs = []             # SSE client-queues
        self.saved_id = None       # id van automatisch opgeslagen roast (na DROP)
        self.source_mode = "starting"   # chip | wachten | sim — wat de bron nu doet

    # ---- abonnees ----
    def subscribe(self):
        q = queue.Queue(maxsize=200)
        with self.lock:
            self.subs.append(q)
        return q

    def unsubscribe(self, q):
        with self.lock:
            if q in self.subs:
                self.subs.remove(q)

    def broadcast(self, msg):
        data = json.dumps(msg)
        with self.lock:
            for q in list(self.subs):
                try:
                    q.put_nowait(data)
                except queue.Full:
                    pass

    def set_source(self, mode):
        with self.lock:
            if self.source_mode == mode:
                return
            self.source_mode = mode
        self.broadcast({"k": "status", "mode": mode})

    def snapshot(self):
        with self.lock:
            return {"k": "snapshot",
                    "version": VERSION,
                    "source_mode": self.source_mode,
                    "samples": list(self.samples),
                    "events": dict(self.events)}

    # ---- metingen ----
    def add_reading(self, et_c, bt_c):
        now = time.monotonic()
        with self.lock:
            self.et, self.bt = et_c, bt_c
            if self.charge is None:
                # voor CHARGE: alleen de uitlezing tonen, nog niet plotten
                self.broadcast_live(et_c, bt_c, None)
                return
            t = now - self.charge
            ror = self._ror(bt_c, t)
            self.ror = ror
            last = self.samples[-1] if self.samples else None
            if not last or t - last["t"] >= 0.5:
                self.samples.append({"t": round(t, 1), "et": round(et_c, 1), "bt": round(bt_c, 1)})
            self.broadcast_live(et_c, bt_c, ror, t)

    def broadcast_live(self, et_c, bt_c, ror, t=None):
        self.broadcast({"k": "reading", "t": round(t, 1) if t is not None else None,
                        "et": round(et_c, 1), "bt": round(bt_c, 1),
                        "ror": round(ror, 2) if ror is not None else None})

    def _ror(self, bt_now, t_now):
        # graden/min over ~30 s venster
        ref = None
        for s in reversed(self.samples):
            if t_now - s["t"] >= 25:
                ref = s; break
        if not ref:
            ref = self.samples[0] if self.samples else None
        if not ref or t_now - ref["t"] <= 0:
            return None
        return (bt_now - ref["bt"]) / ((t_now - ref["t"]) / 60.0)

    # ---- events ----
    def event(self, etype):
        etype = etype.upper()
        if etype == "RESET":
            with self.lock:
                self.samples, self.events, self.charge = [], {}, None
                self.bt = self.et = self.ror = None
                self.saved_id = None
            self.broadcast({"k": "reset"})
            return
        now = time.monotonic()
        if etype == "CHARGE":
            with self.lock:
                self.charge = now
                self.samples = []
                self.events = {"CHARGE": 0}
                self.saved_id = None
            self.broadcast({"k": "reset"})
            self.broadcast({"k": "event", "type": "CHARGE", "t": 0})
            return
        with self.lock:
            t = (now - self.charge) if self.charge else 0
            self.events[etype] = round(t, 1)
        self.broadcast({"k": "event", "type": etype, "t": round(t, 1)})
        if etype == "DROP":
            autosave()


STATE = State()

# --------------------------------------------------------------------------
# Opslag (SQLite, alleen stdlib) — bewaart roasts + importeert Artisan .alog
# --------------------------------------------------------------------------
_dblock = threading.Lock()

def _db():
    con = sqlite3.connect(DB_PATH); con.row_factory = sqlite3.Row; return con

def db_init():
    with _dblock, _db() as con:
        con.execute("""CREATE TABLE IF NOT EXISTS roasts(
            id INTEGER PRIMARY KEY AUTOINCREMENT, created TEXT, title TEXT, bean TEXT,
            weight_in REAL, weight_out REAL, unit TEXT, notes TEXT,
            events TEXT, samples TEXT, source TEXT)""")

def _summary(r):
    ev = json.loads(r["events"] or "{}"); sm = json.loads(r["samples"] or "[]")
    drop, dry, fcs = ev.get("DROP"), ev.get("DRY"), ev.get("FCS")
    dtr = round((drop - fcs) / drop * 100, 1) if (drop and fcs and drop > 0) else None
    loss = None
    if r["weight_in"] and r["weight_out"] and r["weight_in"] > 0:
        loss = round((1 - r["weight_out"] / r["weight_in"]) * 100, 1)
    return {"id": r["id"], "created": r["created"], "title": r["title"] or "Roast",
            "bean": r["bean"] or "", "unit": r["unit"] or "C",
            "weight_in": r["weight_in"], "weight_out": r["weight_out"],
            "notes": r["notes"] or "", "source": r["source"] or "",
            "drop": drop, "dry": dry, "fcs": fcs, "dtr": dtr, "loss": loss, "n": len(sm)}

def db_insert(rec):
    with _dblock, _db() as con:
        cur = con.execute("""INSERT INTO roasts
            (created,title,bean,weight_in,weight_out,unit,notes,events,samples,source)
            VALUES(?,?,?,?,?,?,?,?,?,?)""",
            (rec["created"], rec["title"], rec["bean"], rec["weight_in"], rec["weight_out"],
             rec["unit"], rec["notes"], json.dumps(rec["events"]), json.dumps(rec["samples"]),
             rec["source"]))
        return cur.lastrowid

def db_update_meta(rid, meta):
    fields, vals = [], []
    for k in ("title", "bean", "weight_in", "weight_out", "notes"):
        if k in meta: fields.append(k + "=?"); vals.append(meta[k])
    if not fields: return
    vals.append(rid)
    with _dblock, _db() as con:
        con.execute("UPDATE roasts SET %s WHERE id=?" % ",".join(fields), vals)

def db_list():
    with _dblock, _db() as con:
        rows = con.execute("SELECT * FROM roasts ORDER BY id DESC").fetchall()
    return [_summary(r) for r in rows]

def db_get(rid):
    with _dblock, _db() as con:
        r = con.execute("SELECT * FROM roasts WHERE id=?", (rid,)).fetchone()
    if not r: return None
    d = _summary(r)
    d["samples"] = json.loads(r["samples"] or "[]")
    d["events"] = json.loads(r["events"] or "{}")
    return d

def save_current(meta=None, source="live"):
    """Bewaar de huidige roast (samples + events) met optionele metadata."""
    meta = meta or {}
    with STATE.lock:
        samples = list(STATE.samples); events = dict(STATE.events)
    now = datetime.datetime.now()
    rec = {"created": now.isoformat(timespec="seconds"),
           "title": meta.get("title") or ("Roast " + now.strftime("%d-%m %H:%M")),
           "bean": meta.get("bean", ""), "weight_in": meta.get("weight_in"),
           "weight_out": meta.get("weight_out"), "unit": meta.get("unit", "C"),
           "notes": meta.get("notes", ""), "events": events, "samples": samples,
           "source": source}
    return db_insert(rec)

def autosave():
    """Automatisch opslaan bij DROP, zodat een roast nooit verloren gaat."""
    try:
        with STATE.lock:
            already = STATE.saved_id; n = len(STATE.samples)
        if already or n < 5:
            return
        rid = save_current(source="live")
        with STATE.lock:
            STATE.saved_id = rid
        print("[dreamline] roast automatisch opgeslagen als #%d" % rid)
    except Exception as e:
        print("[dreamline] kon roast niet opslaan: %s" % e)

def import_alog(path):
    """Lees een Artisan .alog-bestand en zet 'm in de database."""
    txt = open(path, encoding="utf-8", errors="replace").read()
    return import_alog_text(txt, os.path.basename(path))

def import_alog_text(txt, name="roast"):
    """Parse een Artisan .alog (Python-repr of JSON) uit tekst en zet 'm in de database."""
    try:
        d = json.loads(txt)
    except Exception:
        d = ast.literal_eval(txt)
    timex = d.get("timex") or []; t1 = d.get("temp1") or []; t2 = d.get("temp2") or []
    ti = d.get("timeindex") or []
    mode = (d.get("mode") or "C").upper()
    toC = (lambda v: (v - 32) * 5 / 9) if mode == "F" else (lambda v: v)
    ci = ti[0] if ti else 0
    if not isinstance(ci, int) or ci < 0 or ci >= max(1, len(timex)): ci = 0
    t0 = timex[ci] if timex else 0
    samples = []
    for i in range(ci, min(len(timex), len(t1), len(t2))):
        et, bt = t1[i], t2[i]
        try:
            if et is None or bt is None: continue
            if et <= -1 or bt <= -1 or et > 800 or bt > 800: continue
            tt = timex[i] - t0
            if tt < 0: continue
            samples.append({"t": round(tt, 1), "et": round(toC(et), 1), "bt": round(toC(bt), 1)})
        except Exception:
            continue
    def evt(pos):
        if len(ti) > pos and isinstance(ti[pos], int) and 0 < ti[pos] < len(timex):
            return round(timex[ti[pos]] - t0, 1)
        return None
    events = {"CHARGE": 0}
    for key, pos in (("DRY", 1), ("FCS", 2), ("FCE", 3), ("DROP", 6)):
        v = evt(pos)
        if v is not None: events[key] = v
    w = d.get("weight") or [None, None, "g"]
    try: win = float(w[0]) if w and w[0] not in (None, "", 0) else None
    except Exception: win = None
    try: wout = float(w[1]) if len(w) > 1 and w[1] not in (None, "", 0) else None
    except Exception: wout = None
    if d.get("roastepoch"):
        created = datetime.datetime.fromtimestamp(d["roastepoch"]).isoformat(timespec="seconds")
    else:
        created = (d.get("roastisodate") or datetime.datetime.now().isoformat(timespec="seconds"))
    title = d.get("title") or (d.get("beans") or name).split("\n")[0][:80]
    rec = {"created": created, "title": title, "bean": (d.get("beans") or "").split("\n")[0][:120],
           "weight_in": win, "weight_out": wout, "unit": "C", "notes": "geimporteerd uit Artisan",
           "events": events, "samples": samples, "source": "import"}
    return db_insert(rec)

# --------------------------------------------------------------------------
# Bronnen: simulator en echte Phidget 1048
# --------------------------------------------------------------------------
def sim_loop():
    """Realistische roastcurve, zodat alles zonder hardware te testen is."""
    print("[dreamline] simulatiemodus actief (geen chip nodig)")
    STATE.set_source("sim")
    STATE.event("CHARGE")
    t = 0.0
    while True:
        t += 2.0
        if t < 75:
            bt = 205 - (205 - 92) * (t / 75) ** 0.8
        else:
            x = min((t - 75) / 585, 1.0)
            bt = 92 + (208 - 92) * (1 - (1 - x) ** 1.7)
        bt += math.sin(t / 24) * 0.4
        et = bt + 38 + 26 * math.exp(-t / 180)
        STATE.add_reading(et, bt)
        if t >= 300 and "DRY" not in STATE.events: STATE.event("DRY")
        if t >= 540 and "FCS" not in STATE.events: STATE.event("FCS")
        if t >= 660 and "DROP" not in STATE.events: STATE.event("DROP")
        time.sleep(0.4)   # 2 roast-sec per 0.4s = vlotte demo


def _tc(tc_type):
    from Phidget22.ThermocoupleType import ThermocoupleType
    return {"J": ThermocoupleType.THERMOCOUPLE_TYPE_J,
            "K": ThermocoupleType.THERMOCOUPLE_TYPE_K,
            "E": ThermocoupleType.THERMOCOUPLE_TYPE_E,
            "T": ThermocoupleType.THERMOCOUPLE_TYPE_T}.get(tc_type.upper(),
            ThermocoupleType.THERMOCOUPLE_TYPE_K)


def phidget_scan(tc_type, seconds=25):
    """Live monitor van alle kanalen, zo herken je welke voeler ET en welke BT is."""
    try:
        from Phidget22.Devices.TemperatureSensor import TemperatureSensor
    except Exception as e:
        print("[dreamline] Phidget-bibliotheek niet gevonden (%s)." % e)
        print("            Installeer met:  pip install Phidget22")
        return
    sensors = []   # [ch, sensor, start_temp, last_temp]
    for ch in range(4):
        try:
            s = TemperatureSensor(); s.setChannel(ch); s.openWaitForAttachment(3000)
            try: s.setThermocoupleType(_tc(tc_type))
            except Exception: pass
            try: s.setDataInterval(max(s.getMinDataInterval(), 200))
            except Exception: pass
            sensors.append([ch, s, None, None])
        except Exception:
            pass
    if not sensors:
        print("[dreamline] Geen kanalen geopend. Staat de chip aan? Sluit eventueel Artisan en probeer opnieuw.")
        return

    found = ", ".join("kanaal %d" % x[0] for x in sensors)
    print("\n  Gevonden: %s" % found)
    print("  " + "-" * 56)
    print("  TIP: warm NU een voeler op (vasthouden of warme lucht erbij).")
    print("       Het kanaal dat STIJGT is precies die voeler.")
    print("       ET = lucht (meestal warmer/sneller), BT = in de bonen.")
    print("       '>' markeert het kanaal dat op dit moment het hardst stijgt.")
    print("  Je hebt ~%d seconden...\n" % seconds)

    def read(s):
        try:
            v = s.getTemperature()
            return v if -50 < v < 1300 else None
        except Exception:
            return None

    t_end = time.time() + seconds
    while time.time() < t_end:
        rises = []
        for x in sensors:
            v = read(x[1])
            if x[2] is None and v is not None: x[2] = v
            x[3] = v
            rises.append(((v - x[2]) if (v is not None and x[2] is not None) else -999, x[0]))
        rises.sort(reverse=True)
        top = rises[0][1] if rises[0][0] > 0.3 else None
        cells = []
        for x in sensors:
            ch, v = x[0], x[3]
            if v is None:
                cells.append("  k%d: --geen voeler--" % ch); continue
            d = (v - x[2]) if x[2] is not None else 0.0
            mark = ">" if ch == top else " "
            cells.append("%sk%d:%6.1fC (%+.1f)" % (mark, ch, v, d))
        sys.stdout.write("\r  " + "   ".join(cells) + "   ")
        sys.stdout.flush()
        time.sleep(0.25)

    print("\n")
    print("  Klaar. Het kanaal dat steeg toen je opwarmde, is die voeler.")
    print("  Onthoud welk kanaal BT (boon) is en welk ET (lucht), en start dan met:")
    print("     python dreamline.py --bt-ch <BT> --et-ch <ET> --tc %s" % tc_type.upper())
    print("  (Of gebruik de starter en vul de nummers in.)\n")
    for x in sensors:
        try: x[1].close()
        except Exception: pass


def phidget_loop(bt_ch, et_ch, tc_type, period=0.5):
    """Echte uitlezing van de Phidget 1048."""
    try:
        from Phidget22.Devices.TemperatureSensor import TemperatureSensor
    except Exception as e:
        print("[dreamline] Phidget-bibliotheek niet gevonden (%s) - terugval op simulatie." % e)
        print("            Installeer met:  pip install Phidget22")
        return sim_loop()

    def mk(ch, naam):
        s = TemperatureSensor(); s.setChannel(ch)
        s.openWaitForAttachment(5000)
        try: s.setThermocoupleType(_tc(tc_type))
        except Exception: pass
        try: s.setDataInterval(max(s.getMinDataInterval(), 250))
        except Exception: pass
        return s

    try:
        bt_s = mk(bt_ch, "BT")
    except Exception as e:
        print("[dreamline] Kon BT-kanaal %d niet openen (%s)." % (bt_ch, e))
        print("            Tip: 'python dreamline.py --scan' toont welke kanalen werken. Terugval op simulatie.")
        return sim_loop()
    try:
        et_s = mk(et_ch, "ET")
    except Exception as e:
        print("[dreamline] Kon ET-kanaal %d niet openen (%s) - terugval op simulatie." % (et_ch, e))
        return sim_loop()

    print("[dreamline] live uitlezing Phidget 1048 (BT=kanaal %d, ET=kanaal %d, type %s)"
          % (bt_ch, et_ch, tc_type))
    try:
        bt0, et0 = bt_s.getTemperature(), et_s.getTemperature()
        print("[dreamline] eerste meting - BT %.1f C, ET %.1f C" % (bt0, et0))
        if abs(bt0 - et0) < 6 and bt0 < 60:
            print("            (allebei rond kamertemperatuur - prima voor CHARGE)")
    except Exception:
        pass

    warned = False; n = 0
    while True:
        try:
            et = et_s.getTemperature(); bt = bt_s.getTemperature()
            STATE.add_reading(et, bt)
            n += 1
            if not warned and n > 20 and (et > 90 or bt > 90) and et < bt - 5:
                print("[dreamline] LET OP: ET (%.0f C) is lager dan BT (%.0f C). Tijdens het roosten "
                      "hoort de luchttemperatuur juist hoger te zijn." % (et, bt))
                print("            Staan de kanalen misschien omgewisseld? Stop (Ctrl+C) en "
                      "wissel --bt-ch en --et-ch om.")
                warned = True
        except Exception as e:
            print("[dreamline] leesfout: %s" % e)
        time.sleep(period)


def sensor_loop(bt_ch, et_ch, tc_type, period=0.5):
    """Houdt de Phidget continu in de gaten en schakelt vanzelf om.
    - Chip aangesloten -> live uitlezing.
    - Chip nog niet / losgekoppeld -> 'wachten op chip' (geen nepdata).
    Plug-and-play: aansluiten tijdens het draaien werkt, zonder herstart."""
    try:
        from Phidget22.Devices.TemperatureSensor import TemperatureSensor
    except Exception as e:
        print("[dreamline] Phidget-bibliotheek niet gevonden (%s) - simulatie." % e)
        return sim_loop()

    attached = {"bt": False, "et": False}

    def _both():
        return attached["bt"] and attached["et"]

    def on_attach(which):
        def handler(ch):
            attached[which] = True
            try: ch.setThermocoupleType(_tc(tc_type))
            except Exception: pass
            try: ch.setDataInterval(max(ch.getMinDataInterval(), 250))
            except Exception: pass
            if _both():
                STATE.set_source("chip")
                print("[dreamline] chip verbonden - live uitlezing actief.")
        return handler

    def on_detach(which):
        def handler(ch):
            attached[which] = False
            STATE.set_source("wachten")
            print("[dreamline] chip losgekoppeld - wachten tot hij er weer in zit.")
        return handler

    bt_s = TemperatureSensor(); bt_s.setChannel(bt_ch)
    et_s = TemperatureSensor(); et_s.setChannel(et_ch)
    bt_s.setOnAttachHandler(on_attach("bt")); bt_s.setOnDetachHandler(on_detach("bt"))
    et_s.setOnAttachHandler(on_attach("et")); et_s.setOnDetachHandler(on_detach("et"))

    STATE.set_source("wachten")
    print("[dreamline] wachten op de chip - sluit de Phidget aan, omschakelen gaat vanzelf.")
    try:
        bt_s.open(); et_s.open()        # niet-blokkerend: koppelt vanzelf, ook later
    except Exception as e:
        print("[dreamline] kon de kanalen niet openen (%s) - simulatie." % e)
        return sim_loop()

    warned = False; n = 0
    while True:
        if _both():
            try:
                et = et_s.getTemperature(); bt = bt_s.getTemperature()
                STATE.add_reading(et, bt); n += 1
                if not warned and n > 20 and (et > 90 or bt > 90) and et < bt - 5:
                    print("[dreamline] LET OP: ET (%.0f) lager dan BT (%.0f) - kanalen mogelijk "
                          "omgewisseld. Gebruik [2] scannen of wissel --bt-ch/--et-ch." % (et, bt))
                    warned = True
            except Exception:
                pass
        time.sleep(period)


# --------------------------------------------------------------------------
# Webserver
# --------------------------------------------------------------------------
def _ver_tuple(v):
    out = []
    for p in str(v).split("."):
        try: out.append(int(p))
        except Exception: out.append(0)
    return tuple(out) or (0,)

def _manifest_url():
    try:
        if CONFIG_PATH.exists():
            u = (json.loads(CONFIG_PATH.read_text(encoding="utf-8")) or {}).get("manifest_url")
            if u: return str(u).strip()
    except Exception:
        pass
    return None

def _feedback_url():
    try:
        if CONFIG_PATH.exists():
            u = (json.loads(CONFIG_PATH.read_text(encoding="utf-8")) or {}).get("feedback_url")
            if u: return str(u).strip()
    except Exception:
        pass
    return None

def _url_allowed(u):
    """Alleen https, of http naar localhost (voor testen/offline LAN-mirror)."""
    try:
        p = urllib.parse.urlparse(u)
        if p.scheme == "https": return True
        if p.scheme == "http" and (p.hostname in ("127.0.0.1", "localhost")): return True
    except Exception:
        pass
    return False

def _fetch(u, cap):
    req = urllib.request.Request(u, headers={"User-Agent": "Dreamline/%s" % VERSION})
    with urllib.request.urlopen(req, timeout=8) as r:
        return r.read(cap + 1)

def check_update():
    out = {"configured": False, "reachable": False, "current": VERSION,
           "latest": None, "update_available": False, "notes": "", "error": ""}
    u = _manifest_url()
    if not u:
        return out
    out["configured"] = True
    if not _url_allowed(u):
        out["error"] = "manifest-URL niet toegestaan (gebruik https)"; return out
    try:
        m = json.loads(_fetch(u, 256 * 1024).decode("utf-8", "replace"))
        out["reachable"] = True
        out["latest"] = str(m.get("version", ""))
        out["notes"] = str(m.get("notes", ""))
        out["update_available"] = _ver_tuple(out["latest"]) > _ver_tuple(VERSION)
    except Exception as e:
        out["error"] = str(e)
    return out

def apply_update():
    """Download nieuwe versie, valideer, maak back-up, schrijf. Niets-of-alles."""
    u = _manifest_url()
    if not u or not _url_allowed(u):
        return {"ok": False, "error": "updates niet (juist) ingesteld"}
    base = u.rsplit("/", 1)[0]
    try:
        m = json.loads(_fetch(u, 256 * 1024).decode("utf-8", "replace"))
    except Exception as e:
        return {"ok": False, "error": "manifest niet leesbaar: %s" % e}
    latest = str(m.get("version", ""))
    files = [f for f in (m.get("files") or []) if f in UPDATABLE]
    if not files:
        return {"ok": False, "error": "manifest bevat geen geldige bestanden"}
    blobs = {}
    for f in files:                                   # 1) downloaden + valideren in geheugen
        fu = base + "/" + f
        if not _url_allowed(fu):
            return {"ok": False, "error": "bestand-URL niet toegestaan: %s" % f}
        try:
            data = _fetch(fu, MAX_FILE)
        except Exception as e:
            return {"ok": False, "error": "download mislukt (%s): %s" % (f, e)}
        if not data or len(data) > MAX_FILE:
            return {"ok": False, "error": "bestand leeg of te groot: %s" % f}
        if f.endswith(".py"):
            try: compile(data.decode("utf-8"), f, "exec")
            except Exception as e:
                return {"ok": False, "error": "nieuwe %s bevat een fout - update afgebroken (%s)" % (f, e)}
        if f == "index.html" and b"Dreamline" not in data:
            return {"ok": False, "error": "index.html lijkt ongeldig - update afgebroken"}
        blobs[f] = data
    try:                                              # 2) back-up van huidige bestanden
        BACKUP_DIR.mkdir(exist_ok=True)
        for f in UPDATABLE:
            src = HERE / f
            if src.exists(): shutil.copy2(src, BACKUP_DIR / f)
        (BACKUP_DIR / "meta.json").write_text(json.dumps(
            {"version": VERSION, "date": datetime.datetime.now().isoformat(timespec="seconds")}),
            encoding="utf-8")
    except Exception as e:
        return {"ok": False, "error": "back-up mislukt - update afgebroken (%s)" % e}
    try:                                              # 3) pas nu de nieuwe bestanden wegschrijven
        for f, data in blobs.items():
            (HERE / f).write_bytes(data)
    except Exception as e:
        return {"ok": False, "error": "schrijven mislukt: %s" % e}
    return {"ok": True, "from": VERSION, "to": latest, "files": list(blobs.keys())}

def rollback_update():
    if not BACKUP_DIR.exists():
        return {"ok": False, "error": "geen back-up gevonden"}
    have = [f for f in UPDATABLE if (BACKUP_DIR / f).exists()]
    if not have:
        return {"ok": False, "error": "back-up onvolledig"}
    prev = None
    try:
        prev = json.loads((BACKUP_DIR / "meta.json").read_text(encoding="utf-8")).get("version")
    except Exception:
        pass
    try:
        for f in have:
            shutil.copy2(BACKUP_DIR / f, HERE / f)
    except Exception as e:
        return {"ok": False, "error": "terugzetten mislukt: %s" % e}
    return {"ok": True, "to": prev or "vorige versie", "files": have}

def _restart_soon(delay=1.5):
    def go():
        try: os.execv(sys.executable, [sys.executable] + sys.argv)
        except Exception: os._exit(0)
    threading.Timer(delay, go).start()


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _send(self, code, ctype, body=b""):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if body:
            self.wfile.write(body)

    def _local(self):
        host = (self.client_address[0] if self.client_address else "") or ""
        return host in ("127.0.0.1", "::1", "localhost") or host.startswith("127.")

    def do_GET(self):
        try:
            path = self.path.split("?")[0]
            if path in ("/", "/index.html", "/index.htm"):
                try:
                    self._send(200, "text/html; charset=utf-8", INDEX.read_bytes())
                except FileNotFoundError:
                    self._send(500, "text/plain", b"index.html niet gevonden naast dreamline.py")
            elif path == "/state":
                self._send(200, "application/json", json.dumps(STATE.snapshot()).encode())
            elif path == "/events":
                self._sse()
            elif path == "/info":
                self._send(200, "application/json", json.dumps({"url": self._url(), "feedback_url": _feedback_url() or ""}).encode())
            elif path == "/api/version":
                self._send(200, "application/json", json.dumps({"version": VERSION}).encode())
            elif path == "/api/update/check":
                self._send(200, "application/json", json.dumps(check_update()).encode())
            elif path == "/qr.svg":
                if qr is None:
                    self._send(500, "text/plain", b"qr.py ontbreekt naast dreamline.py")
                else:
                    self._send(200, "image/svg+xml; charset=utf-8", qr.svg(self._url()).encode())
            elif path == "/api/roasts":
                self._send(200, "application/json", json.dumps(db_list()).encode())
            elif path.startswith("/api/roasts/"):
                try:
                    rid = int(path.rsplit("/", 1)[1])
                except ValueError:
                    return self._send(404, "text/plain", b"not found")
                d = db_get(rid)
                if d is None:
                    self._send(404, "application/json", b'{"error":"niet gevonden"}')
                else:
                    self._send(200, "application/json", json.dumps(d).encode())
            else:
                self._send(404, "text/plain", b"not found")
        except (BrokenPipeError, ConnectionResetError):
            pass
        except Exception:
            try: self._send(500, "application/json", b'{"error":"interne fout"}')
            except Exception: pass

    def _url(self):
        return "http://%s:%d" % (lan_ip(), self.server.server_address[1])

    def _sse(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        q = STATE.subscribe()
        try:
            self.wfile.write(b"retry: 2000\n\n")
            self.wfile.write(("data: " + json.dumps(STATE.snapshot()) + "\n\n").encode())
            self.wfile.flush()
            while True:
                try:
                    msg = q.get(timeout=15)
                    self.wfile.write(("data: " + msg + "\n\n").encode())
                except queue.Empty:
                    self.wfile.write(b": ping\n\n")   # keep-alive
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass
        finally:
            STATE.unsubscribe(q)

    def do_POST(self):
        try:
            try:
                n = int(self.headers.get("Content-Length", 0) or 0)
            except ValueError:
                n = 0
            if n > MAX_BODY:
                return self._send(413, "application/json", b'{"ok":false,"error":"verzoek te groot"}')
            raw = self.rfile.read(n) if n else b"{}"
            try:
                data = json.loads(raw or b"{}")
                if not isinstance(data, dict): data = {}
            except Exception:
                data = {}
            if self.path == "/event":
                STATE.event(str(data.get("type", "")))
                self._send(200, "application/json", b'{"ok":true}')
            elif self.path == "/save":
                meta = {}
                for k in ("title", "bean", "notes"):
                    if data.get(k) is not None: meta[k] = str(data[k])[:2000]
                for k in ("weight_in", "weight_out"):
                    try:
                        if data.get(k) not in (None, ""): meta[k] = float(data[k])
                    except (TypeError, ValueError): pass
                if data.get("unit"): meta["unit"] = str(data["unit"])
                with STATE.lock:
                    rid = STATE.saved_id
                if rid:
                    db_update_meta(rid, meta)
                else:
                    rid = save_current(meta=meta, source="live")
                    with STATE.lock:
                        STATE.saved_id = rid
                self._send(200, "application/json", json.dumps({"ok": True, "id": rid}).encode())
            elif self.path == "/import":
                try:
                    rid = import_alog_text(str(data.get("text", "")), str(data.get("name", "roast")))
                    self._send(200, "application/json", json.dumps({"ok": True, "id": rid}).encode())
                except Exception as e:
                    self._send(200, "application/json", json.dumps({"ok": False, "error": str(e)}).encode())
            elif self.path == "/api/update/apply":
                if not self._local():
                    return self._send(403, "application/json",
                                      b'{"ok":false,"error":"bijwerken kan alleen op de laptop"}')
                r = apply_update()
                self._send(200, "application/json", json.dumps(r).encode())
                if r.get("ok"): _restart_soon()
            elif self.path == "/api/update/rollback":
                if not self._local():
                    return self._send(403, "application/json",
                                      b'{"ok":false,"error":"terugzetten kan alleen op de laptop"}')
                r = rollback_update()
                self._send(200, "application/json", json.dumps(r).encode())
                if r.get("ok"): _restart_soon()
            else:
                self._send(404, "text/plain", b"not found")
        except (BrokenPipeError, ConnectionResetError):
            pass
        except Exception:
            try: self._send(500, "application/json", b'{"ok":false,"error":"interne fout"}')
            except Exception: pass


def lan_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80)); ip = s.getsockname()[0]; s.close()
        return ip
    except Exception:
        return "127.0.0.1"


def main():
    try: sys.stdout.reconfigure(encoding="utf-8")   # voorkomt console-crashes op Windows
    except Exception: pass
    ap = argparse.ArgumentParser(description="Dreamline roast-dashboard")
    ap.add_argument("--port", type=int, default=8080)
    ap.add_argument("--sim", action="store_true", help="forceer simulatie")
    ap.add_argument("--bt-ch", type=int, default=1, help="Phidget-kanaal voor boontemperatuur")
    ap.add_argument("--et-ch", type=int, default=0, help="Phidget-kanaal voor luchttemperatuur")
    ap.add_argument("--tc", default="K", help="type thermokoppel (J/K/E/T)")
    ap.add_argument("--scan", action="store_true", help="toon alle 4 kanalen om ET/BT te vinden")
    ap.add_argument("--import", dest="import_path", metavar="PAD",
                    help="importeer Artisan .alog (bestand of map) en stop")
    ap.add_argument("--no-open", action="store_true",
                    help="open de browser met de QR-code niet automatisch")
    ap.add_argument("--version", action="store_true", help="toon versie en stop")
    ap.add_argument("--check-update", action="store_true", help="controleer op updates en stop")
    ap.add_argument("--rollback", action="store_true", help="zet de vorige versie terug en stop")
    args = ap.parse_args()

    if args.version:
        print("Dreamline %s" % VERSION); return
    if args.check_update:
        r = check_update()
        if not r["configured"]: print("Updates zijn nog niet ingesteld (update_config.json ontbreekt).")
        elif not r["reachable"]: print("Kon niet controleren:", r.get("error") or "geen verbinding")
        elif r["update_available"]: print("Nieuwe versie beschikbaar: %s (jij hebt %s)" % (r["latest"], r["current"]))
        else: print("Je hebt de nieuwste versie (%s)." % r["current"])
        return
    if args.rollback:
        r = rollback_update()
        print("Teruggezet naar %s." % r["to"] if r.get("ok") else "Rollback mislukt: %s" % r.get("error"))
        return

    if args.scan:
        return phidget_scan(args.tc)

    db_init()

    if args.import_path:
        p = args.import_path
        if os.path.isdir(p):
            files = sorted(glob.glob(os.path.join(p, "**", "*.alog"), recursive=True))
        elif os.path.exists(p):
            files = [p]
        else:
            print("Pad niet gevonden:", p); return
        if not files:
            print("Geen .alog-bestanden gevonden in", p); return
        ok = 0
        for f in files:
            try:
                rid = import_alog(f); ok += 1
                print("  geimporteerd: %s  -> #%d" % (os.path.basename(f), rid))
            except Exception as e:
                print("  overgeslagen:  %s  (%s)" % (os.path.basename(f), e))
        print("\n  %d van %d .alog-bestanden in roasts.db gezet.\n" % (ok, len(files)))
        return

    target = sim_loop if args.sim else (lambda: sensor_loop(args.bt_ch, args.et_ch, args.tc))
    threading.Thread(target=target, daemon=True).start()

    try:
        srv = ThreadingHTTPServer(("0.0.0.0", args.port), Handler)
    except OSError as e:
        print("\n  Kon poort %d niet openen (%s)." % (args.port, e))
        print("  Probeer een andere poort:  python dreamline.py --port 8090\n")
        return
    srv.daemon_threads = True
    ip = lan_ip()
    print("\n  Dreamline %s draait." % VERSION)
    print("  Op deze laptop:   http://localhost:%d" % args.port)
    print("  Op de iPad:       http://%s:%d   (zelfde wifi -> Safari -> Zet op beginscherm)" % (ip, args.port))
    print("  Koppelen:         open op de laptop http://localhost:%d en scan de QR met de iPad.\n" % args.port)
    if not args.no_open:
        threading.Timer(1.0, lambda: _safe_open("http://localhost:%d" % args.port)).start()
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\n  Gestopt.")


def _safe_open(url):
    try:
        webbrowser.open(url)
    except Exception:
        pass


if __name__ == "__main__":
    main()
