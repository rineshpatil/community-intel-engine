import base64
import hashlib
import hmac
import json

import pytest

SECRET = "s3cret"


def sign(body: bytes, secret: str = SECRET) -> str:
    return "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


@pytest.fixture
def wh(monkeypatch):
    """Configured webhook module with a clean secret cache."""
    monkeypatch.setenv("CIE_TABLE_NAME", "t")
    monkeypatch.setenv("CIE_QUEUE_URL", "u")
    monkeypatch.setenv("CIE_GITHUB_WEBHOOK_SECRET", SECRET)
    monkeypatch.setenv("CIE_GITHUB_BOT_ID", "42")
    from community_intel.config import get_settings

    get_settings.cache_clear()
    from community_intel.handlers import webhook

    monkeypatch.setattr(webhook, "_secret_cache", None)
    yield webhook
    get_settings.cache_clear()


def test_verify_signature_accepts_valid():
    from community_intel.handlers.webhook import verify_signature
    body = b'{"a":1}'
    assert verify_signature(body, sign(body), SECRET) is True


def test_verify_signature_rejects_tampered_body():
    from community_intel.handlers.webhook import verify_signature
    assert verify_signature(b'{"a":2}', sign(b'{"a":1}'), SECRET) is False


@pytest.mark.parametrize("header", [None, "", "abc", "sha1=deadbeef"])
def test_verify_signature_rejects_malformed_header(header):
    from community_intel.handlers.webhook import verify_signature
    assert verify_signature(b"x", header, SECRET) is False


def test_handler_rejects_bad_signature(wh):
    resp = wh.handler(
        {
            "headers": {
                "x-github-event": "issues",
                "x-hub-signature-256": "sha256=wrong",
            },
            "body": "{}",
            "isBase64Encoded": False,
        },
        None,
    )
    assert resp["statusCode"] == 401


def test_handler_verifies_against_raw_base64_body(wh, monkeypatch):
    # API Gateway may deliver the body base64-encoded. The HMAC must be
    # computed over the decoded raw bytes, never over a re-serialised dict.
    stored, queued = [], []
    monkeypatch.setattr(wh, "put_item_if_new", lambda i: (stored.append(i), True)[1])
    monkeypatch.setattr(wh, "enqueue", lambda i: queued.append(i))

    payload = {
        "action": "opened",
        "issue": {
            "id": 500, "number": 7, "title": "Crash on startup",
            "body": "It crashes every time I open the settings panel.",
            "html_url": "https://example.invalid/7",
            "created_at": "2026-09-01T12:00:00Z",
            "user": {"login": "octocat"},
        },
        "repository": {"full_name": "acme/widget"},
        "sender": {"id": 1},
    }
    raw = json.dumps(payload).encode()

    resp = wh.handler(
        {
            "headers": {
                "x-github-event": "issues",
                "x-hub-signature-256": sign(raw),
            },
            "body": base64.b64encode(raw).decode(),
            "isBase64Encoded": True,
        },
        None,
    )

    assert resp["statusCode"] == 200
    assert stored[0].item_id == "github#500"
    assert queued == ["github#500"]


def test_handler_stores_but_does_not_enqueue_rejected_item(wh, monkeypatch):
    stored, queued = [], []
    monkeypatch.setattr(wh, "put_item_if_new", lambda i: (stored.append(i), True)[1])
    monkeypatch.setattr(wh, "enqueue", lambda i: queued.append(i))

    payload = {
        "action": "opened",
        "issue": {
            "id": 501, "number": 8, "title": None, "body": "+1",
            "html_url": "https://example.invalid/8",
            "created_at": "2026-09-01T12:00:00Z",
            "user": {"login": "octocat"},
        },
        "repository": {"full_name": "acme/widget"},
        "sender": {"id": 1},
    }
    raw = json.dumps(payload).encode()

    resp = wh.handler(
        {
            "headers": {
                "x-github-event": "issues",
                "x-hub-signature-256": sign(raw),
            },
            "body": raw.decode(),
            "isBase64Encoded": False,
        },
        None,
    )

    assert resp["statusCode"] == 200
    assert len(stored) == 1        # the record is kept for tuning
    assert queued == []            # but no pipeline invocation is spent
