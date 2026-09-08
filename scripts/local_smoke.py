#!/usr/bin/env python3
"""End-to-end local smoke test — no AWS account required.

Drives real GitHub webhook payloads through the actual Lambda handler with
moto standing in for DynamoDB and SQS, then reports what landed where. This
exercises signature verification, adapter mapping, idempotency and the
prefilter together, which the unit tests only cover in isolation.
"""
import hashlib
import hmac
import json
import os
import sys

os.environ.update(
    AWS_DEFAULT_REGION="us-east-1",
    AWS_ACCESS_KEY_ID="test",
    AWS_SECRET_ACCESS_KEY="test",
    CIE_TABLE_NAME="items",
    CIE_GITHUB_WEBHOOK_SECRET="local-smoke-secret",
    CIE_GITHUB_BOT_ID="424242",
    CIE_GITHUB_BOT_LOGIN="community-intel[bot]",
    CIE_REDDIT_SUBREDDITS='["widget"]',
    # Explicitly empty: Settings also reads .env, and a deployed .env would
    # otherwise point this at a real SSM parameter. This script must depend on
    # nothing outside itself.
    CIE_GITHUB_WEBHOOK_SECRET_PARAM="",
)

import boto3  # noqa: E402
from moto import mock_aws  # noqa: E402

SECRET = os.environ["CIE_GITHUB_WEBHOOK_SECRET"]


def sign(raw: bytes) -> str:
    return "sha256=" + hmac.new(SECRET.encode(), raw, hashlib.sha256).hexdigest()


def delivery(event_type: str, payload: dict, *, tamper: bool = False) -> dict:
    raw = json.dumps(payload).encode()
    sig = sign(b"different-body") if tamper else sign(raw)
    return {
        "headers": {"x-github-event": event_type, "x-hub-signature-256": sig},
        "body": raw.decode(),
        "isBase64Encoded": False,
    }


def issue(iid: int, title: str, body: str, login: str = "octocat") -> dict:
    return {
        "action": "opened",
        "issue": {
            "id": iid, "number": iid, "title": title, "body": body,
            "html_url": f"https://github.com/acme/widget/issues/{iid}",
            "created_at": "2026-09-01T12:00:00Z",
            "user": {"login": login},
        },
        "repository": {"full_name": "acme/widget"},
        "sender": {"id": 1},
    }


def comment(cid: int, parent: int, body: str, sender_id: int = 2) -> dict:
    return {
        "action": "created",
        "issue": {"id": parent, "number": parent},
        "comment": {
            "id": cid, "body": body,
            "html_url": f"https://github.com/acme/widget/issues/{parent}#c{cid}",
            "created_at": "2026-09-02T09:00:00Z",
            "user": {"login": "hubot"},
        },
        "repository": {"full_name": "acme/widget"},
        "sender": {"id": sender_id},
    }


SCENARIOS = [
    ("real bug report",            "issues",        issue(500, "Crash on startup", "Segfaults every launch after upgrading to 2.1."), False, "accepted"),
    ("duplicate redelivery",       "issues",        issue(500, "Crash on startup", "Segfaults every launch after upgrading to 2.1."), False, "duplicate"),
    ("contentless '+1' comment",   "issue_comment", comment(900, 500, "+1"),                                                          False, "prefiltered:too_short"),
    ("substantive comment",        "issue_comment", comment(901, 500, "Same here, reproducible on Ubuntu 24.04 every time."),         False, "accepted"),
    ("our own bot's output",       "issue_comment", comment(902, 500, "Filed as #42 by community intel.", sender_id=424242),          False, "prefiltered:self"),
    ("tampered signature",         "issues",        issue(999, "Injected", "Should never be stored."),                                True,  "REJECTED 401"),
]


# --- Reddit -------------------------------------------------------------
# PRAW is stubbed rather than mocked at the HTTP layer: the poller's real
# logic is its cursor handling, and that is what these fakes exercise.


class FakeAuthor:
    def __init__(self, name):
        self.name = name


class FakeSubmission:
    def __init__(self, fullname, title, selftext):
        self.name = fullname
        self.title = title
        self.selftext = selftext
        self.permalink = f"/r/widget/comments/{fullname}/x/"
        self.created_utc = 1788000000.0
        self.author = FakeAuthor("ada")
        self.subreddit = FakeSubreddit._singleton


class FakeComment:
    def __init__(self, fullname, parent, body):
        self.name = fullname
        self.body = body
        self.link_id = parent
        self.permalink = f"/r/widget/comments/{parent}/x/{fullname}/"
        self.created_utc = 1788003600.0
        self.author = FakeAuthor("grace")
        self.subreddit = FakeSubreddit._singleton


class FakeSubreddit:
    _singleton = None

    def __init__(self):
        self.display_name = "widget"
        FakeSubreddit._singleton = self

    def new(self, limit=None):
        return [
            FakeSubmission("t3_aaa", "Crash after 2.1 upgrade",
                           "Segfaults on every launch since upgrading."),
            FakeSubmission("t3_bbb", "Dark mode request",
                           "Would really like a dark theme for long sessions."),
        ]

    def comments(self, limit=None):
        return [
            FakeComment("t1_ccc", "t3_aaa",
                        "Confirmed on Ubuntu 24.04, happens every single time."),
            FakeComment("t1_ddd", "t3_aaa", "same"),   # contentless
        ]


class FakeReddit:
    def __init__(self):
        self._sub = FakeSubreddit()

    def subreddit(self, name):
        return self._sub


def run_reddit() -> int:
    from community_intel.handlers import reddit_poll

    reddit_poll._client = lambda: FakeReddit()

    first = reddit_poll.handler({}, None)["ingested"]
    second = reddit_poll.handler({}, None)["ingested"]

    print("\nReddit poller")
    print("-" * 86)
    print(f"first poll ingested  : {first}   "
          "(2 submissions + 1 substantive comment; 'same' prefiltered)")
    print(f"second poll ingested : {second}   "
          "(cursors held - nothing reprocessed)")

    failures = 0
    if first != 3:
        print(f"  NO  expected 3 on first poll, got {first}")
        failures += 1
    if second != 0:
        print(f"  NO  expected 0 on second poll, got {second} "
              "- cursor is not holding")
        failures += 1

    # Cursors must be per listing kind: t3_ and t1_ prefixes are disjoint, so a
    # shared cursor could never match the comment listing.
    subs = reddit_poll.read_cursor("widget", "submissions")
    coms = reddit_poll.read_cursor("widget", "comments")
    print(f"cursor submissions   : {subs}")
    print(f"cursor comments      : {coms}")
    if subs != "t3_aaa" or coms != "t1_ccc":
        print("  NO  cursors not tracked per listing kind")
        failures += 1
    return failures


def main() -> int:
    with mock_aws():
        boto3.client("dynamodb").create_table(
            TableName="items",
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"},
                       {"AttributeName": "sk", "KeyType": "RANGE"}],
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"},
                                  {"AttributeName": "sk", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        qurl = boto3.client("sqs").create_queue(QueueName="work")["QueueUrl"]
        os.environ["CIE_QUEUE_URL"] = qurl

        from community_intel.config import get_settings
        get_settings.cache_clear()
        from community_intel.handlers import webhook

        print(f"{'scenario':<28} {'result':<24} {'expected':<24} ok")
        print("-" * 86)
        failures = 0
        for label, etype, payload, tamper, expected in SCENARIOS:
            resp = webhook.handler(delivery(etype, payload, tamper=tamper), None)
            got = (f"REJECTED {resp['statusCode']}" if resp["statusCode"] != 200
                   else json.loads(resp["body"])["status"])
            ok = got == expected
            failures += not ok
            print(f"{label:<28} {got:<24} {expected:<24} {'yes' if ok else 'NO'}")

        failures += run_reddit()

        stored = boto3.client("dynamodb").scan(TableName="items")["Items"]
        depth = boto3.client("sqs").get_queue_attributes(
            QueueUrl=qurl, AttributeNames=["ApproximateNumberOfMessages"]
        )["Attributes"]["ApproximateNumberOfMessages"]

        items = [i for i in stored if i["pk"]["S"].startswith("ITEM#")]
        cursors = [i for i in stored if i["pk"]["S"].startswith("CURSOR#")]

        print(f"\nGitHub deliveries : {len(SCENARIOS)}")
        print(f"Items stored      : {len(items)}   "
              "(tampered rejected at signature; duplicate wrote no second row)")
        print(f"Cursor rows       : {len(cursors)}   "
              "(one per subreddit per listing kind)")
        print(f"SQS depth         : {depth}   "
              "(only substantive items cost a pipeline invocation)")
        print("\nstored items:")
        for it in sorted(items, key=lambda i: i["pk"]["S"]):
            print(f"  {it['pk']['S']}")
        print("cursor rows:")
        for c in sorted(cursors, key=lambda i: i["sk"]["S"]):
            print(f"  {c['pk']['S']}  {c['sk']['S']} -> {c['last_fullname']['S']}")

        return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
