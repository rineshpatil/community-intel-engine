from datetime import datetime, timezone

from community_intel.models import NormalizedItem

_BASE = "https://www.reddit.com"


def _author(obj) -> str:
    # Deleted accounts come back as None from PRAW.
    author = getattr(obj, "author", None)
    return getattr(author, "name", None) or "[deleted]"


def _common(obj) -> dict:
    return {
        "source": "reddit",
        "channel": f"r/{obj.subreddit.display_name}",
        "url": f"{_BASE}{obj.permalink}",
        "author": _author(obj),
        "created_at": datetime.fromtimestamp(obj.created_utc, tz=timezone.utc),
        "ingested_at": datetime.now(timezone.utc),
    }


def from_submission(sub) -> NormalizedItem:
    return NormalizedItem(
        source_kind="submission",
        external_id=sub.name,          # fullname, e.g. t3_abc123
        parent_external_id=None,
        title=sub.title,
        body=sub.selftext or "",
        raw={"fullname": sub.name, "kind": "submission"},
        **_common(sub),
    )


def from_comment(comment) -> NormalizedItem:
    return NormalizedItem(
        source_kind="comment",
        external_id=comment.name,      # fullname, e.g. t1_def456
        parent_external_id=comment.link_id,   # already a t3_ fullname
        title=None,
        body=comment.body or "",
        raw={"fullname": comment.name, "kind": "comment"},
        **_common(comment),
    )
