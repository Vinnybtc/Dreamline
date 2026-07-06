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


def test_check_update_unconfigured(monkeypatch):
    monkeypatch.setattr(d, "_manifest_url", lambda: None)
    r = d.check_update()
    assert r["configured"] is False
    assert r["update_available"] is False


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


def test_fetch_rejects_https_to_http_redirect(monkeypatch):
    class Resp:
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def geturl(self): return "http://evil.example/payload"   # gedowngraded
        def read(self, n=0): return b"x"

    monkeypatch.setattr(d.urllib.request, "urlopen", lambda req, timeout=0: Resp())
    with pytest.raises(ValueError):
        d._fetch("https://ok.test/manifest.json", 1024)
