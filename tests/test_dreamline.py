"""Regression-safety net for Dreamline.

These tests are deliberately hermetic (no network, no hardware) and target the
exact failure modes seen in practice:

* a release where dreamline.py / index.html / manifest.json drifted apart
  (e.g. 1.6.10 vs 1.6.8);
* a lost `def` that left a called function undefined (the _url_allowed regression),
  which py_compile happily accepts but breaks at runtime;
* string-vs-numeric version comparison (1.6.10 must be newer than 1.6.8);
* feedback forwarding accidentally hitting the network or leaking a private webhook.
"""

import json
import re
from pathlib import Path

import pytest

import dreamline as d  # conftest.py puts the repo root on sys.path

ROOT = Path(__file__).resolve().parent.parent


def _read(name):
    return (ROOT / name).read_text(encoding="utf-8")


# --------------------------------------------------------------------------
# Version consistency — would have caught the 1.6.10 / 1.6.8 mismatch
# --------------------------------------------------------------------------
def test_versions_in_sync():
    py = re.search(r'^VERSION\s*=\s*"([^"]+)"', _read("dreamline.py"), re.M)
    html = re.search(r'APP_VERSION\s*=\s*"([^"]+)"', _read("index.html"))
    manifest = json.loads(_read("manifest.json"))
    assert py, "VERSION not found in dreamline.py"
    assert html, "APP_VERSION not found in index.html"
    assert py.group(1) == html.group(1) == manifest["version"], (
        "version mismatch: dreamline.py=%s index.html=%s manifest=%s"
        % (py.group(1), html.group(1), manifest["version"])
    )


def test_manifest_files_exist_and_are_updatable():
    manifest = json.loads(_read("manifest.json"))
    files = manifest.get("files", [])
    assert files, "manifest lists no files"
    for f in files:
        assert (ROOT / f).exists(), "manifest lists a missing file: %s" % f
        assert f in d.UPDATABLE, "%s is not in UPDATABLE (updater would skip it)" % f


def test_shipped_config_has_blank_feedback_url():
    # The repo must never commit a private webhook; each laptop sets its own.
    cfg = json.loads(_read("update_config.json"))
    assert cfg.get("feedback_url", "") == "", "feedback_url must stay empty in the repo"


# --------------------------------------------------------------------------
# Core API present — guards "lost def" regressions (the _url_allowed bug)
# --------------------------------------------------------------------------
def test_core_api_present():
    for fn in ("check_update", "apply_update", "rollback_update", "forward_feedback",
               "save_feedback", "_url_allowed", "_manifest_url", "_feedback_url",
               "_ver_tuple", "_fetch"):
        assert callable(getattr(d, fn, None)), "missing or uncallable: %s" % fn


@pytest.mark.parametrize("url,allowed", [
    ("https://raw.githubusercontent.com/Vinnybtc/Dreamline/main/manifest.json", True),
    ("http://localhost:8080/x.json", True),
    ("http://127.0.0.1:8080/x.json", True),
    ("http://evil.example/x", False),
    ("ftp://host/file", False),
    ("not a url", False),
    ("", False),
])
def test_url_allowed(url, allowed):
    assert d._url_allowed(url) is allowed


# --------------------------------------------------------------------------
# Version comparison — numeric, not lexicographic
# --------------------------------------------------------------------------
@pytest.mark.parametrize("latest,current,update_available", [
    ("1.7.0", "1.6.10", True),
    ("1.6.10", "1.6.8", True),     # 10 > 8 numerically (string compare would fail)
    ("1.7.0", "1.7.0", False),
    ("1.6.0", "1.7.0", False),
])
def test_update_available(latest, current, update_available):
    assert (d._ver_tuple(latest) > d._ver_tuple(current)) is update_available


# --------------------------------------------------------------------------
# Config wiring
# --------------------------------------------------------------------------
def test_manifest_url_points_to_repo():
    u = d._manifest_url()
    assert u and "raw.githubusercontent.com/Vinnybtc/Dreamline" in u


def test_feedback_url_falls_back_to_default():
    # update_config.json ships feedback_url="", so the built-in relay must apply.
    assert d._feedback_url() == d.DEFAULT_FEEDBACK_URL
    assert d.DEFAULT_FEEDBACK_URL.startswith("https://")


def test_manifest_candidates_config_first_then_defaults(monkeypatch):
    monkeypatch.setattr(d, "_manifest_url", lambda: "https://eigen.test/manifest.json")
    c = d._manifest_candidates()
    assert c[0] == "https://eigen.test/manifest.json"
    for u in d.DEFAULT_MANIFEST_URLS:
        assert u in c, "ingebouwde fallback ontbreekt: %s" % u


def test_manifest_candidates_without_config_uses_defaults(monkeypatch):
    monkeypatch.setattr(d, "_manifest_url", lambda: None)
    assert d._manifest_candidates() == list(d.DEFAULT_MANIFEST_URLS)


def test_check_update_falls_back_to_mirror(monkeypatch):
    # eerste host (raw.github) faalt -> de spiegel (jsdelivr) moet het overnemen
    monkeypatch.setattr(d, "_manifest_url", lambda: None)

    def fake_fetch(u, cap):
        if "raw.githubusercontent" in u:
            raise OSError("geblokkeerd door netwerk")
        return json.dumps({"version": "99.0.0", "notes": "x", "files": []}).encode()

    monkeypatch.setattr(d, "_fetch", fake_fetch)
    r = d.check_update()
    assert r["reachable"] is True
    assert r["update_available"] is True
    assert "jsdelivr" in r["source"]


def test_check_update_reports_error_when_all_hosts_fail(monkeypatch):
    monkeypatch.setattr(d, "_manifest_url", lambda: None)

    def boom(u, cap):
        raise OSError("geen verbinding")

    monkeypatch.setattr(d, "_fetch", boom)
    r = d.check_update()
    assert r["reachable"] is False
    assert r["update_available"] is False
    assert r["error"]


def test_update_cached_never_hits_network(monkeypatch):
    # De pagina leest '/api/update/check' elke 2 min. Die cache-lezing mag NOOIT
    # het netwerk op, anders kan de app tijdens het roosten blijven hangen.
    d._UPD_CACHE["data"] = None
    monkeypatch.setattr(d, "check_update",
                        lambda: (_ for _ in ()).throw(AssertionError("netwerk tijdens cache-lezing")))
    r = d._update_cached()
    assert r["current"] == d.VERSION
    assert r["reachable"] is False            # niets gecached -> veilig 'controleren...'


def test_update_refresh_fills_cache_that_cached_then_serves(monkeypatch):
    monkeypatch.setattr(d, "check_update",
                        lambda: {"configured": True, "reachable": True, "current": d.VERSION,
                                 "latest": "9.9.9", "update_available": True,
                                 "notes": "n", "error": "", "source": "test"})
    d._UPD_CACHE["data"] = None
    fresh = d._update_refresh()
    assert fresh["latest"] == "9.9.9"
    # daarna serveert de cache hetzelfde, zónder opnieuw het netwerk op te gaan
    monkeypatch.setattr(d, "check_update",
                        lambda: (_ for _ in ()).throw(AssertionError("mag niet opnieuw checken")))
    assert d._update_cached()["latest"] == "9.9.9"


# --------------------------------------------------------------------------
# Feedback forwarding stays hermetic and refuses insecure URLs
# --------------------------------------------------------------------------
class _FakeResp:
    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def read(self, n=0):
        return b"{}"


def test_forward_feedback_posts_over_https(monkeypatch):
    seen = {}

    def fake_urlopen(req, timeout=0):
        seen["url"] = req.full_url
        return _FakeResp()

    monkeypatch.setattr(d, "_feedback_url", lambda: "https://example.test/relay")
    monkeypatch.setattr(d.urllib.request, "urlopen", fake_urlopen)
    assert d.forward_feedback({"text": "hi", "version": d.VERSION}) is True
    assert seen["url"] == "https://example.test/relay"


def test_forward_feedback_refuses_non_https(monkeypatch):
    def boom(*a, **k):
        raise AssertionError("must not POST feedback to a non-https URL")

    monkeypatch.setattr(d, "_feedback_url", lambda: "http://insecure.test/relay")
    monkeypatch.setattr(d.urllib.request, "urlopen", boom)
    assert d.forward_feedback({"text": "hi"}) is False


# --------------------------------------------------------------------------
# Curve smoothing (1.8.0) — EMA must damp sensor noise but track real moves
# --------------------------------------------------------------------------
def test_smoothing_damps_noise():
    s = d.State()
    s.charge = 0.0  # pretend CHARGE happened; use t_override so no clock needed
    import random
    rng = random.Random(42)
    outs = []
    for i in range(80):
        base = 120.0
        noisy = base + rng.uniform(-1.5, 1.5)
        s.add_reading(noisy + 40, noisy, t_override=i * 0.25)
        outs.append(s.bt)
    # after warm-up the smoothed value must sit well inside the noise band
    tail = outs[40:]
    spread = max(tail) - min(tail)
    assert spread < 1.0, "EMA should compress ±1.5°C noise, got spread %.2f" % spread


def test_smoothing_tracks_real_rise():
    # realistische roast-stijging: 15°C/min (0.0625°C per 0.25s-stap)
    s = d.State()
    s.charge = 0.0
    rate = 15.0 / 60.0                  # °C per seconde
    for i in range(240):
        t = i * 0.25
        s.add_reading(100.0 + rate * t + 40, 100.0 + rate * t, t_override=t)
    true_bt = 100.0 + rate * 239 * 0.25
    # theoretische EMA-lag = rate * tau ≈ 0.6°C; ruim binnen 1.5°C blijven
    assert true_bt - s.bt < 1.5, "EMA lags %.2f°C behind a real rise" % (true_bt - s.bt)


def test_reset_clears_smoothing_state():
    s = d.State()
    s.charge = 0.0
    s.add_reading(180.0, 140.0, t_override=0.0)
    s.event("RESET")
    assert s._ema_bt is None and s._ema_et is None and s._ema_ts is None


# --------------------------------------------------------------------------
# Shared reference profile (1.8.0)
# --------------------------------------------------------------------------
def test_reference_in_snapshot_and_broadcast():
    s = d.State()
    q = s.subscribe()
    s.set_reference(7)
    assert s.snapshot()["ref_id"] == 7
    msg = json.loads(q.get_nowait())
    assert msg == {"k": "ref", "id": 7}
    s.set_reference(None)
    assert s.snapshot()["ref_id"] is None


# --------------------------------------------------------------------------
# Security (1.8.0): Host/Origin validation, sha256, redirect guard, no CORS-*
# --------------------------------------------------------------------------
def _handler_with(headers):
    h = d.Handler.__new__(d.Handler)   # geen socket nodig: methodes lezen alleen headers
    h.headers = headers
    return h


@pytest.mark.parametrize("host,ok", [
    ("192.168.1.23:8080", True),
    ("localhost:8080", True),
    ("127.0.0.1:8080", True),
    ("[::1]:8080", True),
    ("", True),                        # curl/HTTP 1.0 zonder Host
    ("evil.example.com:8080", False),  # DNS-rebinding
    ("dreamline.attacker.io", False),
])
def test_host_header_validation(host, ok):
    assert _handler_with({"Host": host})._host_ok() is ok


@pytest.mark.parametrize("origin,host,ok", [
    ("", "192.168.1.23:8080", True),                          # geen Origin = geen browser-cross-site
    ("http://192.168.1.23:8080", "192.168.1.23:8080", True),  # eigen app
    ("https://evil.example", "192.168.1.23:8080", False),     # CSRF vanaf een website
    ("null", "192.168.1.23:8080", False),                     # sandboxed/file
])
def test_origin_validation(origin, host, ok):
    assert _handler_with({"Origin": origin, "Host": host})._origin_ok() is ok


def test_sse_has_no_wildcard_cors():
    src = _read("dreamline.py")
    assert 'Access-Control-Allow-Origin", "*"' not in src, \
        "SSE stream must not be readable by arbitrary websites"


def test_manifest_sha256_matches_files():
    import hashlib
    manifest = json.loads(_read("manifest.json"))
    hashes = manifest.get("sha256") or {}
    assert hashes, "manifest must ship sha256 hashes for update integrity"
    for f in manifest.get("files", []):
        assert f in hashes, "missing sha256 for %s" % f
        got = hashlib.sha256((ROOT / f).read_bytes()).hexdigest()
        assert got == hashes[f], "sha256 mismatch for %s (regenerate manifest)" % f


def test_apply_update_rejects_bad_hash(monkeypatch, tmp_path):
    manifest = {"version": "99.0.0", "files": ["qr.py"],
                "sha256": {"qr.py": "0" * 64}}

    def fake_fetch(u, cap):
        if u.split("?")[0].endswith("manifest.json"):
            return json.dumps(manifest).encode()
        return b"# nep-bestand dat niet bij de hash hoort\n"

    monkeypatch.setattr(d, "_manifest_url", lambda: "https://x.test/manifest.json")
    monkeypatch.setattr(d, "_fetch", fake_fetch)
    r = d.apply_update()
    assert r["ok"] is False and "controlegetal" in r["error"]


# --------------------------------------------------------------------------
# Dubbelklik-start (1.9.6): launcher, icoon en snelkoppeling
# --------------------------------------------------------------------------
def test_launcher_bat_starts_in_own_dir_with_running_python():
    txt = d._launcher_bat_text(r"C:\Dreamline\python\python.exe")
    assert 'cd /d "%~dp0"' in txt, "moet altijd vanuit de app-map starten (data blijft dan werken)"
    assert r'"C:\Dreamline\python\python.exe" dreamline.py' in txt


def _mini_png(w=112, h=112):
    import struct as st
    return (b"\x89PNG\r\n\x1a\n" + st.pack(">I", 13) + b"IHDR"
            + st.pack(">IIBBBBB", w, h, 8, 6, 0, 0, 0) + b"rest")


def test_ico_wraps_png_and_rejects_other():
    png = _mini_png()
    ico = d._ico_from_png(png)
    assert ico is not None and ico[:6] == b"\x00\x00\x01\x00\x01\x00"
    assert ico[22:] == png, "PNG hoort integraal in de ICO-container (offset 22)"
    assert d._ico_from_png(b"geen png") is None and d._ico_from_png(None) is None


def test_logo_png_found_in_shipped_index():
    png = d._logo_png_from_index()
    assert png is not None and png[:8] == b"\x89PNG\r\n\x1a\n"


def test_launcher_noop_on_non_windows():
    assert d.ensure_windows_launcher() is False   # CI/macOS: niets doen, niets stuk


def test_launcher_creates_bat_ico_and_shortcut(monkeypatch, tmp_path):
    monkeypatch.setattr(d.os, "name", "nt")
    monkeypatch.setattr(d, "HERE", tmp_path)
    seen = {}

    def fake_run(cmd, **kw):
        seen["cmd"] = cmd
        seen["vbs"] = open(cmd[-1], encoding="utf-8").read()
        class R: pass
        return R()

    monkeypatch.setattr(d.subprocess, "run", fake_run)
    assert d.ensure_windows_launcher() is True
    bat = (tmp_path / "Dreamline.bat").read_text(encoding="ascii")
    assert "dreamline.py" in bat and 'cd /d "%~dp0"' in bat
    assert (tmp_path / "dreamline.ico").exists(), "logo-icoon hoort gemaakt te worden"
    assert seen["cmd"][0] == "cscript"
    assert 'CreateShortcut(desk & "\\Dreamline.lnk")' in seen["vbs"]
    assert "WindowStyle = 7" in seen["vbs"], "console hoort geminimaliseerd te starten"
    assert str(tmp_path / "Dreamline.bat") in seen["vbs"]


# --------------------------------------------------------------------------
# Storing-herstel (1.9.4): relay-antwoord lezen + niet-aangekomen items opnieuw
# --------------------------------------------------------------------------
class _BodyResp:
    def __init__(self, body): self._b = body
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def read(self, n=0): return self._b


@pytest.mark.parametrize("body,expected", [
    (b'{"ok":true,"stored":true,"forwarded":true,"status":200}', True),
    (b'{"ok":true,"stored":true,"forwarded":false,"status":522}', True),   # opgeslagen = veilig
    (b'{"ok":false,"stored":false,"forwarded":false,"status":522}', False),  # mail-dienst plat, niets bewaard
    (b'niet-json antwoord', True),                                          # eigen webhook: 2xx volstaat
])
def test_forward_feedback_reads_relay_body(monkeypatch, body, expected):
    monkeypatch.setattr(d, "_feedback_url", lambda: "https://relay.test/fn")
    monkeypatch.setattr(d.urllib.request, "urlopen", lambda req, timeout=0: _BodyResp(body))
    assert d.forward_feedback({"text": "hi", "ts": "t"}) is expected


def test_retry_unforwarded_resends_and_marks(monkeypatch, tmp_path):
    fb = tmp_path / "feedback.json"
    ul = tmp_path / "update_log.json"
    fb.write_text(json.dumps([
        {"ts": "1", "text": "al aangekomen", "fwd": True},
        {"ts": "2", "text": "nog niet aangekomen"},
        {"ts": "3", "text": "ook niet", "fwd": False},
    ]), encoding="utf-8")
    ul.write_text(json.dumps([{"ts": "4", "text": "update-melding", "fwd": False}]), encoding="utf-8")
    monkeypatch.setattr(d, "FEEDBACK_PATH", fb)
    monkeypatch.setattr(d, "UPDATE_LOG", ul)
    sent = []
    monkeypatch.setattr(d, "forward_feedback", lambda rec: (sent.append(rec["ts"]), True)[1])
    n = d.retry_unforwarded()
    assert n == 3 and sent == ["2", "3", "4"]
    assert all(r.get("fwd") is True for r in json.loads(fb.read_text(encoding="utf-8")))
    assert json.loads(ul.read_text(encoding="utf-8"))[0]["fwd"] is True


def test_retry_unforwarded_stops_after_two_failures(monkeypatch, tmp_path):
    fb = tmp_path / "feedback.json"
    fb.write_text(json.dumps([{"ts": str(i), "text": "x%d" % i} for i in range(5)]), encoding="utf-8")
    monkeypatch.setattr(d, "FEEDBACK_PATH", fb)
    monkeypatch.setattr(d, "UPDATE_LOG", tmp_path / "geen.json")
    attempts = []
    monkeypatch.setattr(d, "forward_feedback", lambda rec: (attempts.append(1), False)[1])
    d.retry_unforwarded()
    assert len(attempts) == 2, "na 2 mislukkingen stoppen (storing), niet alles afwerken"
    assert all(r.get("fwd") is not True for r in json.loads(fb.read_text(encoding="utf-8")))


# --------------------------------------------------------------------------
# QR / koppel-adres (1.9.0): juiste laptop-IP kiezen
# --------------------------------------------------------------------------
def test_local_ipv4s_prefers_wifi_ranges(monkeypatch):
    fake = ["203.0.113.9", "10.8.0.3", "192.168.1.23", "169.254.7.7", "127.0.0.1"]

    class FakeSock:
        def connect(self, *a): pass
        def getsockname(self): return ("203.0.113.9", 0)
        def close(self): pass

    monkeypatch.setattr(d.socket, "socket", lambda *a, **k: FakeSock())
    monkeypatch.setattr(d.socket, "getaddrinfo",
                        lambda *a, **k: [(None, None, None, None, (ip, 0)) for ip in fake])
    ips = d._local_ipv4s()
    assert ips[0] == "192.168.1.23", "192.168.x hoort voorop (winkel-wifi)"
    assert "127.0.0.1" not in ips and "169.254.7.7" not in ips


def test_lan_ip_prefers_address_remote_devices_actually_used():
    old = d.STATE.last_seen_local_ip
    try:
        d.STATE.last_seen_local_ip = "192.168.5.50"
        assert d.lan_ip() == "192.168.5.50"
    finally:
        d.STATE.last_seen_local_ip = old


def test_fetch_rejects_https_to_http_redirect(monkeypatch):
    class Resp:
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def geturl(self): return "http://evil.example/payload"   # gedowngraded
        def read(self, n=0): return b"x"

    monkeypatch.setattr(d.urllib.request, "urlopen", lambda req, timeout=0: Resp())
    with pytest.raises(ValueError):
        d._fetch("https://ok.test/manifest.json", 1024)
