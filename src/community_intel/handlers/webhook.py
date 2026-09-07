import base64
import hashlib
import hmac
import json
import logging

import boto3

from community_intel.adapters.github import from_webhook
from community_intel.config import get_settings
from community_intel.prefilter import rejection_reason
from community_intel.store import enqueue, put_item_if_new

log = logging.getLogger()
log.setLevel(logging.INFO)

_secret_cache: str | None = None


def _webhook_secret() -> str:
    """Resolve the webhook secret, preferring SSM SecureString.

    Cached at module scope so warm invocations do not re-read the parameter.
    Falls back to the plain setting for local tests.
    """
    global _secret_cache
    if _secret_cache is not None:
        return _secret_cache

    settings = get_settings()
    if settings.github_webhook_secret_param:
        resp = boto3.client("ssm").get_parameter(
            Name=settings.github_webhook_secret_param, WithDecryption=True
        )
        _secret_cache = resp["Parameter"]["Value"]
    else:
        _secret_cache = settings.github_webhook_secret
    return _secret_cache


def verify_signature(raw_body: bytes, header: str | None, secret: str) -> bool:
    if not header or not header.startswith("sha256="):
        return False
    expected = "sha256=" + hmac.new(
        secret.encode(), raw_body, hashlib.sha256
    ).hexdigest()
    # compare_digest, not ==, to avoid leaking the signature via timing.
    return hmac.compare_digest(expected, header)


def _raw_body(event: dict) -> bytes:
    body = event.get("body") or ""
    if event.get("isBase64Encoded"):
        return base64.b64decode(body)
    return body.encode("utf-8")


def _ok(msg: str) -> dict:
    return {"statusCode": 200, "body": json.dumps({"status": msg})}


def handler(event: dict, context) -> dict:
    settings = get_settings()
    headers = {k.lower(): v for k, v in (event.get("headers") or {}).items()}

    # Verify against the raw bytes GitHub signed, never a re-serialised dict.
    raw = _raw_body(event)
    if not verify_signature(
        raw, headers.get("x-hub-signature-256"), _webhook_secret()
    ):
        return {"statusCode": 401, "body": json.dumps({"error": "bad signature"})}

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return {"statusCode": 400, "body": json.dumps({"error": "bad json"})}

    item = from_webhook(headers.get("x-github-event", ""), payload)
    if item is None:
        return _ok("ignored")

    if not put_item_if_new(item):
        log.info("duplicate item_id=%s", item.item_id)
        return _ok("duplicate")

    reason = rejection_reason(
        item,
        bot_id=settings.github_bot_id,
        bot_login=settings.github_bot_login,
    )
    if reason:
        log.info("prefiltered item_id=%s reason=%s", item.item_id, reason)
        return _ok(f"prefiltered:{reason}")

    enqueue(item.item_id)
    log.info("enqueued item_id=%s", item.item_id)
    return _ok("accepted")
