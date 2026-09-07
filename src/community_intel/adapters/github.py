from datetime import datetime, timezone
from typing import Any

from community_intel.models import NormalizedItem

# Only these actions carry new text. "labeled", "closed", "deleted" and friends
# would otherwise re-ingest content we already have.
_ACCEPTED_ACTIONS = {"opened", "created"}


def _parse_ts(value: str) -> datetime:
    # GitHub sends RFC3339 with a trailing Z; normalise it explicitly.
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _login(user: dict[str, Any] | None) -> str:
    # Deliberately reads only "login". Some payloads carry an email address on
    # the same object and it must never enter the store.
    return (user or {}).get("login", "unknown")


def _build(
    *, kind: str, node: dict, payload: dict, parent: str | None
) -> NormalizedItem:
    return NormalizedItem(
        source="github",
        source_kind=kind,
        external_id=str(node["id"]),
        parent_external_id=parent,
        channel=payload["repository"]["full_name"],
        url=node["html_url"],
        author=_login(node.get("user")),
        title=node.get("title"),
        body=node.get("body") or "",
        created_at=_parse_ts(node["created_at"]),
        ingested_at=datetime.now(timezone.utc),
        raw=payload,
    )


def from_webhook(event_type: str, payload: dict) -> NormalizedItem | None:
    if payload.get("action") not in _ACCEPTED_ACTIONS:
        return None

    if event_type == "issues":
        return _build(
            kind="issue", node=payload["issue"], payload=payload, parent=None
        )

    if event_type == "issue_comment":
        return _build(
            kind="issue_comment", node=payload["comment"], payload=payload,
            parent=str(payload["issue"]["id"]),
        )

    if event_type == "discussion":
        return _build(
            kind="discussion", node=payload["discussion"], payload=payload,
            parent=None,
        )

    if event_type == "discussion_comment":
        return _build(
            kind="discussion_comment", node=payload["comment"], payload=payload,
            parent=str(payload["discussion"]["id"]),
        )

    return None
