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
VERSION = "1.7.0"
CONFIG_PATH = HERE / "update_config.json"   # {"manifest_url": "https://.../manifest.json"}
BACKUP_DIR = HERE / "backup"                # vorige versie, voor 1-tik rollback
UPDATABLE = ("index.html", "dreamline.py", "qr.py")   # alleen deze mag een update vervangen
MAX_BODY = 8 * 1024 * 1024                  # max grootte van een POST (DoS-bescherming)
MAX_FILE = 6 * 1024 * 1024                  # max grootte van een te downloaden bestand

# --- Artisan-achtige slimmigheden -----------------------------------------
ROR_SPAN = 14.0        # seconden: venster waarover RoR wordt berekend (delta-span, polyfit)
ROR_CLAMP = (-90.0, 130.0)   # RoR begrenzen op zinnige waarden (°/min)
SPIKE_MAX = 25.0       # °C: grotere sprong tussen 2 metingen = spike -> mediaan-filter vangt 'm
TP_RISE = 3.0          # °C boven het minimum voordat we het keerpunt (TP) vaststellen

PAIRED_PATH = HERE / "paired.json"     # onthoudt of er ooit een iPad is gekoppeld
UPDATE_LOG = HERE / "update_log.json"  # logboek van geïnstalleerde updates (voor jou)

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
        self.raw0 = None                 # laatste ruwe VoltageRatio kanaal 0
        self.raw1 = None                 # laatste ruwe VoltageRatio kanaal 1
        self._bt3 = []                   # laatste 3 ruwe BT-metingen (mediaanfilter tegen spikes)
        self._et3 = []                   # idem voor ET
        self._tp_min = None              # laagste BT na CHARGE (voor keerpunt-detectie)
        self._tp_min_t = None
        self._tp_first = None            # eerste BT na CHARGE (om een echte dip te herkennen)
        self.remote = 0                  # aantal verbonden iPads/telefoons (niet-localhost)

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
    @staticmethod
    def _med3(buf, v):
        buf.append(v)
        if len(buf) > 3:
            buf.pop(0)
        return sorted(buf)[len(buf) // 2]

    def add_reading(self, et_c, bt_c, t_override=None):
        now = time.monotonic()
        with self.lock:
            # spike-onderdrukking (Artisan 'smooth spikes'): mediaan van de laatste 3 metingen.
            # Een losse uitschieter (ruis/contactstoring) verdwijnt; echte bewegingen blijven.
            et_c = self._med3(self._et3, et_c)
            bt_c = self._med3(self._bt3, bt_c)
            self.et, self.bt = et_c, bt_c
            if self.charge is None:
                # voor CHARGE: alleen de uitlezing tonen, nog niet plotten
                self.broadcast_live(et_c, bt_c, None)
                return
            t = t_override if t_override is not None else (now - self.charge)
            last = self.samples[-1] if self.samples else None
            if not last or t - last["t"] >= 0.5:
                self.samples.append({"t": round(t, 1), "et": round(et_c, 1), "bt": round(bt_c, 1)})
            ror = self._ror(bt_c, t)
            self.ror = ror
            self._detect_tp(bt_c, t)
            self.broadcast_live(et_c, bt_c, ror, t)

    def broadcast_live(self, et_c, bt_c, ror, t=None):
        self.broadcast({"k": "reading", "t": round(t, 1) if t is not None else None,
                        "et": round(et_c, 1), "bt": round(bt_c, 1),
                        "ror": round(ror, 2) if ror is not None else None})

    def _ror(self, bt_now, t_now):
        # RoR (°/min) als kleinste-kwadraten helling over een venster van ROR_SPAN seconden
        # (Artisan 'Polyfit'): veel rustiger dan punt-op-punt, met minimale vertraging.
        pts = [(s["t"], s["bt"]) for s in self.samples if 0 <= t_now - s["t"] <= ROR_SPAN]
        if not pts or pts[-1][0] < t_now - 0.05:
            pts.append((t_now, bt_now))
        n = len(pts)
        if n < 3:
            return None
        sx = sum(p[0] for p in pts); sy = sum(p[1] for p in pts)
        sxx = sum(p[0] * p[0] for p in pts); sxy = sum(p[0] * p[1] for p in pts)
        denom = n * sxx - sx * sx
        if denom <= 1e-9:
            return None
        slope = (n * sxy - sx * sy) / denom        # °C per seconde
        ror = slope * 60.0
        lo, hi = ROR_CLAMP
        return max(lo, min(hi, ror))

    def _detect_tp(self, bt_now, t_now):
        # Keerpunt (Turning Point): laagste BT na CHARGE, maar alleen als BT eerst
        # echt is gedaald (koude bonen) en daarna weer voldoende stijgt.
        if "TP" in self.events or "DROP" in self.events:
            return
        if self._tp_first is None:
            self._tp_first = bt_now
        if self._tp_min is None or bt_now < self._tp_min:
            self._tp_min, self._tp_min_t = bt_now, t_now
        dipped = self._tp_first is not None and self._tp_min <= self._tp_first - TP_RISE
        if dipped and t_now > 20 and bt_now >= self._tp_min + TP_RISE:
            self.events["TP"] = round(self._tp_min_t, 1)
            self.broadcast({"k": "event", "type": "TP", "t": round(self._tp_min_t, 1)})

    # ---- events ----
    def event(self, etype):
        etype = etype.upper()
        if etype == "RESET":
            with self.lock:
                self.samples, self.events, self.charge = [], {}, None
                self.bt = self.et = self.ror = None
                self.saved_id = None
                self._bt3 = []; self._et3 = []
                self._tp_min = None; self._tp_min_t = None; self._tp_first = None
            self.broadcast({"k": "reset"})
            return
        now = time.monotonic()
        if etype == "CHARGE":
            with self.lock:
                self.charge = now
                self.samples = []
                self.events = {"CHARGE": 0}
                self.saved_id = None
                self._tp_min = None; self._tp_min_t = None; self._tp_first = None
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
        STATE.add_reading(et, bt, t)
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


def phidget_scan(tc_type, seconds=25, rtd=True):
    """Live monitor van alle kanalen, zo herken je welke voeler ET en welke BT is."""
    try:
        if rtd:
            from Phidget22.Devices.VoltageRatioInput import VoltageRatioInput as Channel
        else:
            from Phidget22.Devices.TemperatureSensor import TemperatureSensor as Channel
    except Exception as e:
        print("[dreamline] Phidget-bibliotheek niet gevonden (%s)." % e)
        print("            Installeer met:  pip install Phidget22")
        return
    sensors = []   # [ch, sensor, start_temp, last_temp]
    for ch in range(4):
        try:
            s = Channel(); s.setChannel(ch); s.openWaitForAttachment(3000)
            if not rtd:
                try: s.setThermocoupleType(_tc(tc_type))
                except Exception: pass
            try: s.setDataInterval(max(s.getMinDataInterval(), 200))
            except Exception: pass
            sensors.append([ch, s, None, None])
        except Exception:
            pass
    if not sensors:
        soort = "RTD-bridge (1046)" if rtd else "thermokoppel (1048)"
        print("[dreamline] Geen %s-kanalen geopend. Staat de chip aan? Sluit eventueel Artisan en probeer opnieuw." % soort)
        if rtd:
            print("            (Is het tóch een thermokoppel-kastje? Probeer dan: Start Dreamline + thermokoppel-modus.)")
        return

    found = ", ".join("kanaal %d" % x[0] for x in sensors)
    print("\n  Gevonden (%s): %s" % ("RTD/1046" if rtd else "TC/1048", found))
    print("  " + "-" * 56)
    print("  TIP: warm NU een voeler op (vasthouden of warme lucht erbij).")
    print("       Het kanaal dat STIJGT is precies die voeler.")
    print("       ET = lucht (meestal warmer/sneller), BT = in de bonen.")
    print("       '>' markeert het kanaal dat op dit moment het hardst stijgt.")
    print("  Je hebt ~%d seconden...\n" % seconds)

    def read(s):
        try:
            v = rtd_ratio_to_temp(s.getVoltageRatio()) if rtd else s.getTemperature()
            return v if (v is not None and -50 < v < 1300) else None
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


# --------------------------------------------------------------------------
# RTD-omrekening (Phidget 1046 + 3175 voltage divider, PT100)
# --------------------------------------------------------------------------
# Deze waarden komen overeen met de Artisan-instelling: PT100, Div, gain 1.
RTD_R0      = 100.0     # PT100 = 100 ohm bij 0 C (PT1000 = 1000)
RTD_REF_OHM = 1993.0    # referentieweerstand voltage divider (uit ijking tegen Artisan)
RTD_GAIN    = 1.0       # bridge-gain (staat in Artisan op 1)
_RTD_A = 3.9083e-3      # Callendar-Van Dusen A (PT100)
_RTD_B = -5.775e-7      # Callendar-Van Dusen B (PT100)

# Kalibratie die op de laptop bewaard blijft (overleeft updates):
CAL_PATH = HERE / "dreamline_cal.json"
CAL = {"swap": False, "rref0": RTD_REF_OHM, "rref1": RTD_REF_OHM}

def load_cal():
    try:
        d = json.loads(CAL_PATH.read_text(encoding="utf-8"))
        for k in ("swap", "rref0", "rref1"):
            if k in d: CAL[k] = d[k]
    except Exception:
        pass

def save_cal():
    try:
        CAL_PATH.write_text(json.dumps(CAL), encoding="utf-8")
        return True
    except Exception:
        return False

def _rtd_res_at(t):
    """PT100 weerstand (ohm) bij temperatuur t (C)."""
    return RTD_R0 * (1.0 + _RTD_A * t + _RTD_B * t * t)

def solve_rref(bv, t_true):
    """Leid de referentieweerstand af uit een bekende temperatuur (1-punts ijking)."""
    if bv is None or bv <= 0 or bv >= 1:
        return None
    return _rtd_res_at(t_true) * (1.0 - bv) / bv

def _rtd_res_to_temp(r, r0=None):
    """Weerstand (ohm) -> temperatuur (C) via Callendar-Van Dusen (T >= 0)."""
    r0 = r0 or RTD_R0
    if r is None or r <= 0:
        return None
    disc = _RTD_A * _RTD_A - 4.0 * _RTD_B * (1.0 - r / r0)
    if disc < 0:
        return None
    return (-_RTD_A + math.sqrt(disc)) / (2.0 * _RTD_B)

def rtd_ratio_to_temp(bv, r_ref=None, r0=None, gain=None):
    """VoltageRatio (V/V) van de 1046 -> temperatuur (C), voltage-divider-wiring."""
    if bv is None:
        return None
    r_ref = r_ref or RTD_REF_OHM
    gain = gain or RTD_GAIN
    bv = bv / gain
    d = 1.0 - bv
    if d <= 0:
        return None
    r = r_ref * bv / d          # voltage divider: bv = R/(R+Rref)  ->  R = Rref*bv/(1-bv)
    return _rtd_res_to_temp(r, r0)


def sensor_loop(bt_ch, et_ch, tc_type, period=0.3, rtd=True):
    """Houdt de Phidget continu in de gaten en schakelt vanzelf om.
    RTD (1046): leest kanaal 0 en 1 als VoltageRatio en rekent om naar temperatuur
    (PT100). Welk kanaal BT is en welk ET wordt door CAL['swap'] bepaald, en is in
    de app om te wisselen + te ijken (blijft op de laptop bewaard)."""
    try:
        if rtd:
            from Phidget22.Devices.VoltageRatioInput import VoltageRatioInput as Channel
        else:
            from Phidget22.Devices.TemperatureSensor import TemperatureSensor as Channel
    except Exception as e:
        print("[dreamline] Phidget-bibliotheek niet gevonden (%s) - simulatie." % e)
        return sim_loop()

    load_cal()
    soort = "RTD-bridge (1046, PT100)" if rtd else "thermokoppel (1048, type %s)" % tc_type
    attached = {0: False, 1: False}
    latest = {0: None, 1: None}     # laatste ruwe waarde via change-event (1046 vereist dit)
    last_data = {0: 0.0, 1: 0.0}    # per kanaal: tijdstip van de laatste verse meting (voor de bewaker)
    reopening = {"v": False}        # True tijdens automatisch heropenen (onderdrukt 'wachten'-melding)

    def _both():
        return attached[0] and attached[1]

    def on_change(ch_no):
        def handler(ch, value):
            latest[ch_no] = value
            last_data[ch_no] = time.monotonic()
        return handler

    def on_attach(ch_no):
        def handler(ch):
            attached[ch_no] = True
            if rtd:
                # 1046: bridge AANzetten en elke meting laten doorkomen, anders bevriest de waarde
                try: ch.setBridgeEnabled(True)
                except Exception: pass
                try: ch.setVoltageRatioChangeTrigger(0.0)
                except Exception: pass
            else:
                try: ch.setThermocoupleType(_tc(tc_type))
                except Exception: pass
            try: ch.setDataInterval(max(ch.getMinDataInterval(), 150))
            except Exception: pass
            last_data[ch_no] = time.monotonic()   # stale-klok van dit kanaal start zodra het er is
            if _both():
                STATE.set_source("chip")
                print("[dreamline] chip verbonden - live uitlezing actief (%s)." % soort)
        return handler

    def on_detach(ch_no):
        def handler(ch):
            attached[ch_no] = False
            if not reopening["v"]:
                STATE.set_source("wachten")
                print("[dreamline] chip losgekoppeld - wachten tot hij er weer in zit.")
        return handler

    sens = {}
    for ch_no in (0, 1):
        s = Channel(); s.setChannel(ch_no)
        s.setOnAttachHandler(on_attach(ch_no)); s.setOnDetachHandler(on_detach(ch_no))
        if rtd:
            try: s.setOnVoltageRatioChangeHandler(on_change(ch_no))
            except Exception: pass
        else:
            try: s.setOnTemperatureChangeHandler(on_change(ch_no))
            except Exception: pass
        sens[ch_no] = s

    STATE.set_source("wachten")
    print("[dreamline] wachten op de chip (%s) - aansluiten gaat vanzelf, geen herstart nodig." % soort)
    try:
        for s in sens.values(): s.open()
    except Exception as e:
        print("[dreamline] kon de kanalen niet openen (%s) - simulatie." % e)
        return sim_loop()

    def raw(ch_no):
        # Actief uitvragen elke ronde: pakt ook nieuwe waarden op als het change-event
        # niet (vaak genoeg) afgaat. Lukt dat niet, val terug op de laatste event-waarde.
        try:
            return sens[ch_no].getVoltageRatio() if rtd else sens[ch_no].getTemperature()
        except Exception:
            return latest[ch_no]

    def conv(ch_no, r):
        if r is None:
            return None
        if not rtd:
            return r
        return rtd_ratio_to_temp(r, CAL["rref0"] if ch_no == 0 else CAL["rref1"])

    warned = False; n = 0
    STALE = 2.5          # seconden zonder nieuwe meting voordat we automatisch heropenen
    last_reopen = 0.0
    prev = {0: None, 1: None}
    def _reopen():
        reopening["v"] = True
        for s in sens.values():
            try: s.close()
            except Exception: pass
        time.sleep(0.25)
        for s in sens.values():
            try: s.open()
            except Exception: pass
        last_data[0] = last_data[1] = time.monotonic()
        prev[0] = prev[1] = None
        reopening["v"] = False
    while True:
        if _both():
            try:
                r0, r1 = raw(0), raw(1)
                STATE.raw0, STATE.raw1 = r0, r1
                # leeft de chip? een echte meting ruist altijd iets; verandert er iets op
                # een kanaal, dan stroomt daar verse data — dat kanaal is dan 'vers'.
                nowm = time.monotonic()
                if r0 is not None and r0 != prev[0]: last_data[0] = nowm
                if r1 is not None and r1 != prev[1]: last_data[1] = nowm
                prev[0], prev[1] = r0, r1
                t0, t1 = conv(0, r0), conv(1, r1)
                if t0 is not None and t1 is not None:
                    # standaard: BT = kanaal 0, ET = kanaal 1 ; 'swap' keert dit om
                    bt, et = (t1, t0) if CAL["swap"] else (t0, t1)
                    STATE.add_reading(et, bt); n += 1
                    if not warned and n > 20 and (et > 90 or bt > 90) and et < bt - 5:
                        print("[dreamline] LET OP: ET (%.0f) lager dan BT (%.0f) - gebruik in de app "
                              "'Wissel BT/ET'." % (et, bt))
                        warned = True
            except Exception:
                pass
            # bewaker: staat de meting stil? dan zelf het kanaal heropenen
            # (vervangt het handmatig lostrekken van de USB).
            # bewaker: staat één van de twee voelers stil? dan zelf de verbinding
            # verversen (vervangt het handmatig lostrekken van de USB). Per kanaal,
            # zodat een bevroren BT niet 'verstopt' wordt door een lopende ET.
            now = time.monotonic()
            stale0 = (now - last_data[0]) > STALE
            stale1 = (now - last_data[1]) > STALE
            if (stale0 or stale1) and (now - last_reopen) > STALE:
                last_reopen = now
                which = "beide voelers" if (stale0 and stale1) else ("voeler 1 (kanaal 0)" if stale0 else "voeler 2 (kanaal 1)")
                print("[dreamline] %s stond stil - verbinding wordt automatisch ververst "
                      "(geen USB lostrekken nodig)." % which)
                _reopen()
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

FEEDBACK_PATH = HERE / "feedback.json"

def save_feedback(rec):
    """Bewaar een opmerking lokaal (veilig; gaat nooit verloren)."""
    try:
        data = []
        if FEEDBACK_PATH.exists():
            try: data = json.loads(FEEDBACK_PATH.read_text(encoding="utf-8")) or []
            except Exception: data = []
        data.append(rec)
        FEEDBACK_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=1), encoding="utf-8")
        return True
    except Exception:
        return False

def forward_feedback(rec):
    """Stuur de opmerking door naar de (eigen) webhook in feedback_url, indien ingesteld.
    Alleen https (of localhost). Mislukken is ok: de lokale kopie blijft bewaard."""
    u = _feedback_url()
    if not u or not (u.startswith("https://") or u.startswith("http://localhost")):
        return False
    try:
        body = json.dumps(rec, ensure_ascii=False).encode("utf-8")
        req = urllib.request.Request(u, data=body, method="POST",
                                     headers={"Content-Type": "application/json",
                                              "User-Agent": "Dreamline/%s" % VERSION})
        with urllib.request.urlopen(req, timeout=8) as r:
            r.read(2048)
        return True
    except Exception:
        return False

def log_and_notify_update(to_ver):
    """Bij een geslaagde update: lokaal loggen én actief een seintje naar Vincent sturen
    (via dezelfde veilige webhook als de opmerkingen). Mislukken mag, het logboek blijft."""
    host = socket.gethostname()
    rec = {"ts": datetime.datetime.now().isoformat(timespec="seconds"),
           "type": "update", "from": VERSION, "to": to_ver, "host": host,
           "version": to_ver, "name": "UPDATE · %s" % host,
           "text": "Dreamline is op '%s' bijgewerkt naar v%s." % (host, to_ver)}
    try:
        data = []
        if UPDATE_LOG.exists():
            try: data = json.loads(UPDATE_LOG.read_text(encoding="utf-8")) or []
            except Exception: data = []
        data.append(rec)
        UPDATE_LOG.write_text(json.dumps(data, ensure_ascii=False, indent=1), encoding="utf-8")
    except Exception:
        pass
    try:
        forward_feedback(rec)     # zelfde privé-webhook -> komt in jouw Google Sheet
    except Exception:
        pass

def _ever_paired():
    try:
        return bool(PAIRED_PATH.exists() and (json.loads(PAIRED_PATH.read_text(encoding="utf-8")) or {}).get("paired"))
    except Exception:
        return False

def _mark_paired():
    try:
        if not _ever_paired():
            PAIRED_PATH.write_text(json.dumps(
                {"paired": True, "since": datetime.datetime.now().isoformat(timespec="seconds")}),
                encoding="utf-8")
    except Exception:
        pass


def _url_allowed(u):
    """Alleen https, of http naar localhost (voor testen/offline LAN-mirror)."""
    try:
        p = urllib.parse.urlparse(u)
        if p.scheme == "https": return True
        if p.scheme == "http" and (p.hostname in ("127.0.0.1", "localhost")): return True
    except Exception:
        pass
    return False

def _cb(u):
    """Voeg een wisselende parameter toe zodat GitHub's cache wordt omzeild (verse versie)."""
    sep = "&" if "?" in u else "?"
    return u + sep + "cb=" + str(int(time.time()))

def _fetch(u, cap):
    req = urllib.request.Request(u, headers={"User-Agent": "Dreamline/%s" % VERSION,
                                             "Cache-Control": "no-cache", "Pragma": "no-cache"})
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
        m = json.loads(_fetch(_cb(u), 256 * 1024).decode("utf-8", "replace"))
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
        m = json.loads(_fetch(_cb(u), 256 * 1024).decode("utf-8", "replace"))
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
            data = _fetch(_cb(fu), MAX_FILE)
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
    try:                                              # 4) markeer dat er net is bijgewerkt (voor het controle-scherm)
        (HERE / "just_updated.json").write_text(
            json.dumps({"to": latest, "from": VERSION}), encoding="utf-8")
    except Exception:
        pass
    log_and_notify_update(latest)                     # 5) logboek + actief seintje naar jou
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
                self._send(200, "application/json", json.dumps({
                    "url": self._url(), "feedback_url": _feedback_url() or "",
                    "is_laptop": self._local(),
                    "ever_paired": _ever_paired(),
                    "remote_connected": STATE.remote > 0}).encode())
            elif path == "/api/setup":
                self._send(200, "application/json", json.dumps({
                    "swap": CAL["swap"], "rref0": CAL["rref0"], "rref1": CAL["rref1"],
                    "raw0": STATE.raw0, "raw1": STATE.raw1,
                    "bt": STATE.bt, "et": STATE.et, "source_mode": STATE.source_mode,
                    "feedback_url": _feedback_url() or "", "is_laptop": self._local()}).encode())
            elif path == "/api/version":
                self._send(200, "application/json", json.dumps({"version": VERSION}).encode())
            elif path == "/api/justupdated":
                info = {"updated": False}
                try:
                    p = HERE / "just_updated.json"
                    if p.exists():
                        d = json.loads(p.read_text(encoding="utf-8")) or {}
                        info = {"updated": True, "to": d.get("to", VERSION), "from": d.get("from", "")}
                except Exception:
                    pass
                self._send(200, "application/json", json.dumps(info).encode())
            elif path == "/api/feedback":
                if not self._local():
                    return self._send(403, "application/json", b"[]")
                try:
                    fb = json.loads(FEEDBACK_PATH.read_text(encoding="utf-8")) if FEEDBACK_PATH.exists() else []
                except Exception:
                    fb = []
                self._send(200, "application/json", json.dumps(fb, ensure_ascii=False).encode())
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
        # is dit een iPad/telefoon (niet de laptop zelf)? dan telt het als 'gekoppeld'
        try:
            host = self.client_address[0]
        except Exception:
            host = ""
        is_remote = host not in ("127.0.0.1", "::1", "localhost", "")
        if is_remote:
            with STATE.lock:
                STATE.remote += 1
            _mark_paired()
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
            if is_remote:
                with STATE.lock:
                    STATE.remote = max(0, STATE.remote - 1)

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
            elif self.path == "/api/fbdest":
                if not self._local():
                    return self._send(403, "application/json", b'{"ok":false,"error":"alleen op de laptop"}')
                url = str(data.get("url", "")).strip()
                if url and not (url.startswith("https://") or url.startswith("http://localhost")):
                    return self._send(200, "application/json", b'{"ok":false,"error":"gebruik een https-adres"}')
                try:
                    cfg = {}
                    if CONFIG_PATH.exists():
                        cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8")) or {}
                    cfg["feedback_url"] = url
                    CONFIG_PATH.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
                    self._send(200, "application/json", json.dumps({"ok": True, "url": url}).encode())
                except Exception as e:
                    self._send(200, "application/json", json.dumps({"ok": False, "error": str(e)}).encode())
            elif self.path == "/api/justupdated/clear":
                try:
                    p = HERE / "just_updated.json"
                    if p.exists(): p.unlink()
                except Exception:
                    pass
                self._send(200, "application/json", b'{"ok":true}')
            elif self.path == "/api/feedback":
                text = str(data.get("text", "")).strip()
                if not text:
                    return self._send(200, "application/json", b'{"ok":false,"error":"leeg"}')
                rec = {"ts": datetime.datetime.now().isoformat(timespec="seconds"),
                       "version": VERSION,
                       "name": str(data.get("name", ""))[:80],
                       "text": text[:4000]}
                save_feedback(rec)
                fwd = forward_feedback(rec)
                self._send(200, "application/json", json.dumps({"ok": True, "forwarded": fwd}).encode())
            elif self.path == "/api/swap":
                CAL["swap"] = not CAL["swap"]
                save_cal()
                self._send(200, "application/json", json.dumps({"ok": True, "swap": CAL["swap"]}).encode())
            elif self.path == "/api/calibrate":
                try:
                    bt_true = float(data.get("bt")); et_true = float(data.get("et"))
                except (TypeError, ValueError):
                    return self._send(200, "application/json", b'{"ok":false,"error":"vul beide temperaturen in"}')
                # welk fysiek kanaal is nu BT en welk ET?
                bt_phys = 1 if CAL["swap"] else 0
                et_phys = 0 if CAL["swap"] else 1
                raws = {0: STATE.raw0, 1: STATE.raw1}
                done = []
                for phys, tt in ((bt_phys, bt_true), (et_phys, et_true)):
                    rr = solve_rref(raws.get(phys), tt)
                    if rr and 200 < rr < 20000:
                        CAL["rref%d" % phys] = rr; done.append(phys)
                if len(done) == 2:
                    save_cal()
                    self._send(200, "application/json", json.dumps(
                        {"ok": True, "rref0": round(CAL["rref0"]), "rref1": round(CAL["rref1"])}).encode())
                else:
                    self._send(200, "application/json",
                               b'{"ok":false,"error":"geen geldige meting - staat de chip live?"}')
            elif self.path == "/api/update/apply":
                # Bijwerken mag nu ook vanaf de iPad: het downloaden én installeren
                # gebeurt hoe dan ook op de laptop (deze server draait daar). De iPad
                # stuurt enkel het startsein, de nieuwe versie komt op de laptop terecht.
                r = apply_update()
                self._send(200, "application/json", json.dumps(r).encode())
                if r.get("ok"): _restart_soon()
            elif self.path == "/api/update/rollback":
                # Terugzetten mag ook vanaf de iPad (gebeurt op de laptop).
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
    ap.add_argument("--thermocouple", action="store_true",
                    help="gebruik thermokoppel-ingang (1048) i.p.v. RTD-bridge (1046)")
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
        return phidget_scan(args.tc, rtd=not args.thermocouple)

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

    target = sim_loop if args.sim else (lambda: sensor_loop(args.bt_ch, args.et_ch, args.tc, rtd=not args.thermocouple))
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
