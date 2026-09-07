import logging

import boto3
import praw

from community_intel.adapters.reddit import from_comment, from_submission
from community_intel.config import get_settings
from community_intel.prefilter import rejection_reason
from community_intel.store import enqueue, put_item_if_new

log = logging.getLogger()
log.setLevel(logging.INFO)

PAGE = 100

# Submissions and comments are separate listings with disjoint fullname
# prefixes (t3_ vs t1_), so they need separate cursors. A single shared cursor
# could never match the second listing and it would re-scan every poll.
LISTINGS = (
    ("submissions", lambda s: s.new(limit=PAGE), from_submission),
    ("comments", lambda s: s.comments(limit=PAGE), from_comment),
)


def _table():
    return boto3.resource("dynamodb").Table(get_settings().table_name)


def read_cursor(subreddit: str, kind: str) -> str | None:
    resp = _table().get_item(
        Key={"pk": "CURSOR#reddit", "sk": f"{subreddit}#{kind}"}
    )
    return (resp.get("Item") or {}).get("last_fullname")


def write_cursor(subreddit: str, kind: str, fullname: str) -> None:
    _table().put_item(
        Item={
            "pk": "CURSOR#reddit",
            "sk": f"{subreddit}#{kind}",
            "last_fullname": fullname,
        }
    )


def _client() -> praw.Reddit:
    s = get_settings()
    return praw.Reddit(
        client_id=s.reddit_client_id,
        client_secret=s.reddit_client_secret,
        user_agent=s.reddit_user_agent,
    )


def _ingest(item) -> bool:
    """Store the item, enqueue it only if it survives the prefilter."""
    if not put_item_if_new(item):
        return False
    reason = rejection_reason(item, bot_id=0, bot_login="")
    if reason:
        log.info("prefiltered item_id=%s reason=%s", item.item_id, reason)
        return False
    enqueue(item.item_id)
    return True


def handler(event: dict, context) -> dict:
    settings = get_settings()
    reddit = _client()
    total = 0

    for name in settings.reddit_subreddits:
        sub = reddit.subreddit(name)

        # PRAW's stream.* is a polling loop with an in-memory seen-set that
        # cannot survive a Lambda invocation, so page explicitly instead.
        for kind, listing_of, builder in LISTINGS:
            cursor = read_cursor(name, kind)
            newest: str | None = None

            for obj in listing_of(sub):
                if newest is None:
                    newest = obj.name
                if cursor and obj.name == cursor:
                    break
                if _ingest(builder(obj)):
                    total += 1

            if newest:
                write_cursor(name, kind, newest)

    log.info("reddit poll ingested=%d", total)
    return {"ingested": total}
