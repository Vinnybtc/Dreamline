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
