import re

from community_intel.models import NormalizedItem

MIN_CHARS = 15

_KNOWN_BOTS = {"automoderator"}
_STRUCTURAL_NOISE = re.compile(
    r"^\s*(closed\s+via\s+#?\w+|merged\s+via\s+#?\w+|duplicate\s+of\s+#\d+)\s*$",
    re.IGNORECASE,
)
# Strip markdown, mentions and emoji so "+1" and reactions collapse to nothing.
_STRIP = re.compile(r"[^\w\s]|_", re.UNICODE)


def _is_self(item: NormalizedItem, bot_id: int, bot_login: str) -> bool:
    if bot_login and item.author.lower() == bot_login.lower():
        return True
    sender = item.raw.get("sender") or {}
    return bool(bot_id) and sender.get("id") == bot_id


def _is_other_bot(item: NormalizedItem) -> bool:
    author = item.author.lower()
    return author.endswith("[bot]") or author in _KNOWN_BOTS


def rejection_reason(
    item: NormalizedItem, bot_id: int, bot_login: str
) -> str | None:
    """Return why this item should not be enqueued, or None to accept it.

    Ordered cheapest-first. The self check runs before everything because it is
    the loop guard.
    """
    if _is_self(item, bot_id, bot_login):
        return "self"

    if _is_other_bot(item):
        return "bot"

    body = (item.body or "").strip()

    if _STRUCTURAL_NOISE.match(body):
        return "structural_noise"

    # Signal may live in the title, so measure both together.
    meaningful = _STRIP.sub("", f"{item.title or ''} {body}").strip()
    if len(meaningful) < MIN_CHARS:
        return "too_short"

    return None
