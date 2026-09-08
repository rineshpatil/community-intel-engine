#!/usr/bin/env python3
"""Verify a DEPLOYED stack end to end against real AWS.

Unlike scripts/local_smoke.py this touches your account: it POSTs to the real
API Gateway endpoint and writes real rows to DynamoDB. Reads config from .env
(CIE_API_URL, CIE_TABLE_NAME, CIE_QUEUE_URL) and the webhook secret from SSM.

Test rows use repository "acme/widget" and ids in the 9000 range so they are
easy to identify and remove.
"""
import hashlib
import hmac
import json
import os
import sys
import time
import urllib.error
import urllib.request

import boto3
from dotenv import load_dotenv

load_dotenv()

API = os.environ.get("CIE_API_URL", "").rstrip("/")
TABLE = os.environ.get("CIE_TABLE_NAME", "")
QUEUE = os.environ.get("CIE_QUEUE_URL", "")
PARAM = os.environ.get("CIE_GITHUB_WEBHOOK_SECRET_PARAM", "")

if not all([API, TABLE, QUEUE, PARAM]):
    sys.exit("Set CIE_API_URL, CIE_TABLE_NAME, CIE_QUEUE_URL and "
             "CIE_GITHUB_WEBHOOK_SECRET_PARAM in .env (see cdk deploy outputs).")

SECRET = boto3.client("ssm").get_parameter(
    Name=PARAM, WithDecryption=True
)["Parameter"]["Value"]


def post(event_type: str, payload: dict, tamper: bool = False):
    raw = json.dumps(payload).encode()
    signed = b"tampered" if tamper else raw
    sig = "sha256=" + hmac.new(SECRET.encode(), signed, hashlib.sha256).hexdigest()
    req = urllib.request.Request(
        f"{API}/webhook/github", data=raw, method="POST",
        headers={"Content-Type": "application/json",
                 "X-GitHub-Event": event_type,
                 "X-Hub-Signature-256": sig},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()[:80]


def issue(i, title, body):
    return {"action": "opened",
            "issue": {"id": i, "number": i, "title": title, "body": body,
                      "html_url": f"https://github.com/acme/widget/issues/{i}",
                      "created_at": "2026-09-08T12:00:00Z",
                      "user": {"login": "octocat"}},
            "repository": {"full_name": "acme/widget"}, "sender": {"id": 1}}


def comment(c, p, body):
    return {"action": "created", "issue": {"id": p, "number": p},
            "comment": {"id": c, "body": body,
                        "html_url": f"https://github.com/acme/widget/issues/{p}#c{c}",
                        "created_at": "2026-09-08T12:01:00Z",
                        "user": {"login": "hubot"}},
            "repository": {"full_name": "acme/widget"}, "sender": {"id": 2}}


# Ids are unique per run. Reusing them would make every rerun report
# "duplicate" - correct behaviour from the system, useless as a test.
RUN = int(time.time())
IID, CID, TID = RUN * 10, RUN * 10 + 1, RUN * 10 + 2

BUG = ("Live deploy check", "Real request through API Gateway to Lambda.")
CASES = [
    ("valid signed issue",     "issues",        issue(IID, *BUG),                 False, 200, "accepted"),
    ("same issue redelivered", "issues",        issue(IID, *BUG),                 False, 200, "duplicate"),
    ("contentless comment",    "issue_comment", comment(CID, IID, "+1"),          False, 200, "prefiltered:too_short"),
    ("tampered signature",     "issues",        issue(TID, "No", "Never stored."), True, 401, None),
]


def main() -> int:
    print(f"{'case':<24} {'http':<6} {'status':<24} ok")
    print("-" * 66)
    fails = 0
    for label, et, payload, tamper, want_code, want_status in CASES:
        code, body = post(et, payload, tamper)
        status = body.get("status") if isinstance(body, dict) else "-"
        ok = code == want_code and (want_status is None or status == want_status)
        fails += not ok
        print(f"{label:<24} {code:<6} {str(status):<24} {'yes' if ok else 'NO'}")

    rows = boto3.client("dynamodb").scan(TableName=TABLE)["Items"]
    depth = boto3.client("sqs").get_queue_attributes(
        QueueUrl=QUEUE, AttributeNames=["ApproximateNumberOfMessages"]
    )["Attributes"]["ApproximateNumberOfMessages"]

    print(f"\nDynamoDB rows : {len(rows)}")
    for r in sorted(rows, key=lambda x: x["pk"]["S"]):
        print("  ", r["pk"]["S"])
    print(f"SQS depth     : {depth}")

    # Remove only the rows this run created, so the corpus stays clean and the
    # script is safely repeatable.
    ddb = boto3.client("dynamodb")
    removed = 0
    for ext in (IID, CID):
        ddb.delete_item(
            TableName=TABLE,
            Key={"pk": {"S": f"ITEM#github#{ext}"}, "sk": {"S": "META"}},
        )
        removed += 1
    print(f"cleaned up    : {removed} test rows")

    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
