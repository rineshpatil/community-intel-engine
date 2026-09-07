# Foundation & Ingestion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deploy a serverless ingestion path that receives GitHub webhooks and polls Reddit, normalises both to a common schema, stores every item idempotently in DynamoDB, and enqueues only non-noise items to SQS.

**Architecture:** Two Lambda entrypoints write to one DynamoDB table and one SQS queue. GitHub arrives via API Gateway with HMAC verification; Reddit via EventBridge Scheduler on a two-minute interval. A pure-function prefilter gates SQS enqueueing. No model calls, no containers, no VPC.

**Tech Stack:** Python 3.12, uv, pydantic v2, pydantic-settings, boto3, PRAW, AWS CDK v2 (Python), pytest, moto.

**Spec:** `docs/superpowers/specs/2026-09-08-community-intel-engine-design.md`

## Global Constraints

- Python 3.12. Package name `community_intel`, `src/` layout.
- Dependency management with `uv`. No Docker anywhere, including Lambda packaging.
- `item_id` is always `f"{source}#{external_id}"`. Never random.
- The `author` field carries a username only. Email must never be stored.
- All AWS resources are declared only under `infra/`. `src/` never declares infrastructure.
- IAM policies name explicit actions and resource ARNs. No wildcards.
- Every DynamoDB item write is conditional on `attribute_not_exists(pk)`.
- Git commits carry no Claude attribution or co-author trailer.

## Deviation from the spec (deliberate, one)

The spec places `prefilter` as the first node of the pipeline graph. This plan
runs it at **ingest** instead, gating the SQS enqueue rather than the graph.

Reason: every item is still written to DynamoDB, so the record of what was
rejected is preserved for tuning, but rejected items never become an SQS message
or a Lambda invocation. Same observability, strictly less compute. The pipeline
graph in Plan 2 therefore begins at `classify`.

## File Structure

| File | Responsibility |
|---|---|
| `src/community_intel/config.py` | Settings from environment, one place |
| `src/community_intel/models.py` | `NormalizedItem` and id derivation |
| `src/community_intel/store.py` | DynamoDB and SQS access, nothing else |
| `src/community_intel/prefilter.py` | Pure rejection predicate, no I/O |
| `src/community_intel/adapters/github.py` | Webhook payload to `NormalizedItem` |
| `src/community_intel/adapters/reddit.py` | PRAW object to `NormalizedItem` |
| `src/community_intel/handlers/webhook.py` | API Gateway entrypoint, HMAC verify |
| `src/community_intel/handlers/reddit_poll.py` | EventBridge entrypoint, cursor |
| `infra/app.py`, `infra/stack.py` | CDK stack |
| `scripts/build_lambda.sh` | Docker-free Lambda packaging |

---

### Task 1: Project scaffolding and settings

**Files:**
- Create: `pyproject.toml`
- Create: `src/community_intel/__init__.py`
- Create: `src/community_intel/config.py`
- Test: `tests/test_config.py`

**Interfaces:**
- Consumes: nothing
- Produces: `Settings` (pydantic-settings class), `get_settings() -> Settings`
  cached via `functools.lru_cache`. Fields used by later tasks:
  `table_name: str`, `queue_url: str`, `github_webhook_secret: str`,
  `github_bot_id: int`, `reddit_subreddits: list[str]`,
  `reddit_client_id: str`, `reddit_client_secret: str`,
  `reddit_user_agent: str`.

- [ ] **Step 1: Create `pyproject.toml`**

```toml
[project]
name = "community-intel"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = [
    "pydantic>=2.7,<3",
    "pydantic-settings>=2.2,<3",
    "boto3>=1.34",
    "praw>=7.7,<8",
]

[project.optional-dependencies]
dev = [
    "pytest>=8,<9",
    "moto[dynamodb,sqs]>=5,<6",
    "aws-cdk-lib>=2.160,<3",
    "constructs>=10,<11",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/community_intel"]

[tool.pytest.ini_options]
pythonpath = ["src", "."]
testpaths = ["tests"]
```

- [ ] **Step 2: Write the failing test**

Create `tests/test_config.py`:

```python
import os
import pytest
from community_intel.config import Settings


def test_settings_read_from_env(monkeypatch):
    monkeypatch.setenv("CIE_TABLE_NAME", "items")
    monkeypatch.setenv("CIE_QUEUE_URL", "https://sqs/q")
    monkeypatch.setenv("CIE_GITHUB_WEBHOOK_SECRET", "s3cret")
    monkeypatch.setenv("CIE_GITHUB_BOT_ID", "12345")
    monkeypatch.setenv("CIE_REDDIT_SUBREDDITS", '["python","aws"]')

    s = Settings()

    assert s.table_name == "items"
    assert s.github_bot_id == 12345
    assert s.reddit_subreddits == ["python", "aws"]


def test_missing_required_setting_raises(monkeypatch):
    monkeypatch.delenv("CIE_TABLE_NAME", raising=False)
    monkeypatch.delenv("CIE_QUEUE_URL", raising=False)
    with pytest.raises(Exception):
        Settings()
```

- [ ] **Step 3: Run test to verify it fails**

Run: `uv run pytest tests/test_config.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'community_intel.config'`

- [ ] **Step 4: Write the implementation**

Create `src/community_intel/__init__.py` as an empty file.

Create `src/community_intel/config.py`:

```python
from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # protected_namespaces=() so future MODEL_* settings do not collide with
    # pydantic's reserved "model_" prefix.
    model_config = SettingsConfigDict(
        env_prefix="CIE_", extra="ignore", protected_namespaces=()
    )

    table_name: str
    queue_url: str

    github_webhook_secret: str = ""
    github_webhook_secret_param: str = ""   # SSM parameter name, preferred
    github_bot_id: int = 0
    github_bot_login: str = ""

    reddit_subreddits: list[str] = []
    reddit_client_id: str = ""
    reddit_client_secret: str = ""
    reddit_user_agent: str = "community-intel/0.1"


@lru_cache
def get_settings() -> Settings:
    return Settings()
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/test_config.py -v`
Expected: 2 passed

- [ ] **Step 6: Commit**

```bash
git add pyproject.toml src/community_intel/__init__.py src/community_intel/config.py tests/test_config.py
git commit -m "feat: add project scaffolding and settings"
```

---

### Task 2: NormalizedItem

**Files:**
- Create: `src/community_intel/models.py`
- Test: `tests/test_models.py`

**Interfaces:**
- Consumes: nothing
- Produces: `NormalizedItem` (pydantic `BaseModel`) with fields
  `item_id, source, source_kind, external_id, parent_external_id, channel, url,
  author, title, body, created_at, ingested_at, raw`, plus
  `NormalizedItem.make_id(source: str, external_id: str) -> str` and
  `NormalizedItem.to_dynamo_item() -> dict` which returns a dict containing
  `pk` and `sk` keys ready for `put_item`.

- [ ] **Step 1: Write the failing test**

Create `tests/test_models.py`:

```python
from datetime import datetime, timezone

from community_intel.models import NormalizedItem


def make_item(**overrides) -> NormalizedItem:
    defaults = dict(
        source="github",
        source_kind="issue",
        external_id="998",
        parent_external_id=None,
        channel="acme/widget",
        url="https://github.com/acme/widget/issues/1",
        author="octocat",
        title="Crash on startup",
        body="It crashes when I launch it.",
        created_at=datetime(2026, 9, 1, 12, 0, tzinfo=timezone.utc),
        ingested_at=datetime(2026, 9, 1, 12, 5, tzinfo=timezone.utc),
        raw={"any": "payload"},
    )
    defaults.update(overrides)
    return NormalizedItem(**defaults)


def test_item_id_is_derived_from_source_and_external_id():
    item = make_item()
    assert item.item_id == "github#998"


def test_item_id_is_stable_across_construction():
    assert make_item().item_id == make_item().item_id


def test_make_id_helper_matches_field():
    assert NormalizedItem.make_id("reddit", "t1_abc") == "reddit#t1_abc"


def test_to_dynamo_item_sets_keys_and_json_safe_values():
    d = make_item().to_dynamo_item()
    assert d["pk"] == "ITEM#github#998"
    assert d["sk"] == "META"
    # datetimes must be strings, not datetime objects, for DynamoDB
    assert isinstance(d["created_at"], str)
    assert d["created_at"].startswith("2026-09-01T12:00:00")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_models.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'community_intel.models'`

- [ ] **Step 3: Write the implementation**

Create `src/community_intel/models.py`:

```python
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator


class NormalizedItem(BaseModel):
    item_id: str = ""
    source: Literal["github", "reddit"]
    source_kind: str
    external_id: str
    # Parent issue for a comment; parent submission for a reddit comment.
    parent_external_id: str | None = None
    channel: str
    url: str
    author: str          # username only, never an email address
    title: str | None = None
    body: str
    created_at: datetime
    ingested_at: datetime
    raw: dict[str, Any] = Field(default_factory=dict)

    @staticmethod
    def make_id(source: str, external_id: str) -> str:
        return f"{source}#{external_id}"

    @model_validator(mode="after")
    def _derive_item_id(self) -> "NormalizedItem":
        object.__setattr__(
            self, "item_id", self.make_id(self.source, self.external_id)
        )
        return self

    def to_dynamo_item(self) -> dict[str, Any]:
        # mode="json" converts datetimes to ISO strings, which DynamoDB accepts.
        data = self.model_dump(mode="json")
        data["pk"] = f"ITEM#{self.item_id}"
        data["sk"] = "META"
        return data
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_models.py -v`
Expected: 4 passed

- [ ] **Step 5: Commit**

```bash
git add src/community_intel/models.py tests/test_models.py
git commit -m "feat: add NormalizedItem schema with derived item_id"
```

---

### Task 3: Idempotent store

**Files:**
- Create: `src/community_intel/store.py`
- Test: `tests/test_store.py`

**Interfaces:**
- Consumes: `NormalizedItem`, `get_settings`
- Produces: `put_item_if_new(item: NormalizedItem) -> bool` returning `True` on
  first write and `False` when the item already existed;
  `enqueue(item_id: str) -> None` sending `{"item_id": ...}` as the SQS body.

- [ ] **Step 1: Write the failing test**

Create `tests/test_store.py`:

```python
import json
import os
from datetime import datetime, timezone

import boto3
import pytest
from moto import mock_aws

from community_intel.models import NormalizedItem


@pytest.fixture
def aws_env(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    with mock_aws():
        ddb = boto3.client("dynamodb", region_name="us-east-1")
        ddb.create_table(
            TableName="items",
            KeySchema=[
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        sqs = boto3.client("sqs", region_name="us-east-1")
        url = sqs.create_queue(QueueName="work")["QueueUrl"]

        monkeypatch.setenv("CIE_TABLE_NAME", "items")
        monkeypatch.setenv("CIE_QUEUE_URL", url)

        from community_intel.config import get_settings

        get_settings.cache_clear()
        yield url
        get_settings.cache_clear()


def an_item(external_id="998") -> NormalizedItem:
    now = datetime(2026, 9, 1, tzinfo=timezone.utc)
    return NormalizedItem(
        source="github", source_kind="issue", external_id=external_id,
        channel="acme/widget", url="https://example.invalid/1",
        author="octocat", title="t", body="b",
        created_at=now, ingested_at=now, raw={},
    )


def test_first_write_returns_true(aws_env):
    from community_intel.store import put_item_if_new
    assert put_item_if_new(an_item()) is True


def test_duplicate_write_returns_false(aws_env):
    from community_intel.store import put_item_if_new
    put_item_if_new(an_item())
    assert put_item_if_new(an_item()) is False


def test_different_items_both_written(aws_env):
    from community_intel.store import put_item_if_new
    assert put_item_if_new(an_item("1")) is True
    assert put_item_if_new(an_item("2")) is True


def test_enqueue_sends_item_id(aws_env):
    from community_intel.store import enqueue
    enqueue("github#998")
    sqs = boto3.client("sqs", region_name="us-east-1")
    msgs = sqs.receive_message(QueueUrl=aws_env, MaxNumberOfMessages=1)
    assert json.loads(msgs["Messages"][0]["Body"]) == {"item_id": "github#998"}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_store.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'community_intel.store'`

- [ ] **Step 3: Write the implementation**

Create `src/community_intel/store.py`:

```python
import json

import boto3
from botocore.exceptions import ClientError

from community_intel.config import get_settings
from community_intel.models import NormalizedItem


def _table():
    return boto3.resource("dynamodb").Table(get_settings().table_name)


def _sqs():
    return boto3.client("sqs")


def put_item_if_new(item: NormalizedItem) -> bool:
    """Write the item. Return False if it was already present.

    This is the single duplicate-suppression mechanism for ingestion. It runs
    before any model call, so repeat webhook deliveries and overlapping poll
    windows cost nothing beyond one conditional write.
    """
    try:
        _table().put_item(
            Item=item.to_dynamo_item(),
            ConditionExpression="attribute_not_exists(pk)",
        )
        return True
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return False
        raise


def enqueue(item_id: str) -> None:
    _sqs().send_message(
        QueueUrl=get_settings().queue_url,
        MessageBody=json.dumps({"item_id": item_id}),
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_store.py -v`
Expected: 4 passed

- [ ] **Step 5: Commit**

```bash
git add src/community_intel/store.py tests/test_store.py
git commit -m "feat: add idempotent DynamoDB write and SQS enqueue"
```

---

### Task 4: Prefilter

**Files:**
- Create: `src/community_intel/prefilter.py`
- Test: `tests/test_prefilter.py`

**Interfaces:**
- Consumes: `NormalizedItem`
- Produces: `rejection_reason(item: NormalizedItem, bot_id: int, bot_login: str) -> str | None`
  returning a short reason string when the item should NOT be enqueued, or
  `None` when it should be. Callers treat any non-`None` as "store but do not
  enqueue".

- [ ] **Step 1: Write the failing test**

Create `tests/test_prefilter.py`:

```python
from datetime import datetime, timezone

import pytest

from community_intel.models import NormalizedItem
from community_intel.prefilter import rejection_reason

NOW = datetime(2026, 9, 1, tzinfo=timezone.utc)


def item(**kw) -> NormalizedItem:
    base = dict(
        source="github", source_kind="issue", external_id="1",
        channel="acme/widget", url="https://example.invalid/1",
        author="octocat", title="Crash on startup",
        body="The application crashes every time I open the settings panel.",
        created_at=NOW, ingested_at=NOW, raw={},
    )
    base.update(kw)
    return NormalizedItem(**base)


def test_normal_item_is_accepted():
    assert rejection_reason(item(), bot_id=999, bot_login="cie[bot]") is None


def test_our_own_bot_is_rejected_by_login():
    # The self-loop guard. Our own issues and comments fire webhooks; without
    # this the system ingests its own output forever.
    assert rejection_reason(
        item(author="cie[bot]"), bot_id=999, bot_login="cie[bot]"
    ) == "self"


def test_our_own_bot_is_rejected_by_sender_id():
    assert rejection_reason(
        item(raw={"sender": {"id": 999}}), bot_id=999, bot_login="cie[bot]"
    ) == "self"


def test_other_bots_are_rejected():
    assert rejection_reason(
        item(author="dependabot[bot]"), bot_id=999, bot_login="cie[bot]"
    ) == "bot"


def test_automoderator_is_rejected():
    assert rejection_reason(
        item(source="reddit", author="AutoModerator"),
        bot_id=999, bot_login="cie[bot]",
    ) == "bot"


@pytest.mark.parametrize("body", ["+1", "👍", "   ", "same", ""])
def test_contentless_bodies_are_rejected(body):
    assert rejection_reason(
        item(body=body, title=None), bot_id=999, bot_login="cie[bot]"
    ) == "too_short"


def test_closed_via_commit_reference_is_rejected():
    assert rejection_reason(
        item(body="Closed via #123", title=None),
        bot_id=999, bot_login="cie[bot]",
    ) == "structural_noise"


def test_short_body_with_long_title_is_accepted():
    # An issue whose signal lives in the title must survive.
    assert rejection_reason(
        item(title="Segfault when parsing empty config file", body="see title"),
        bot_id=999, bot_login="cie[bot]",
    ) is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_prefilter.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'community_intel.prefilter'`

- [ ] **Step 3: Write the implementation**

Create `src/community_intel/prefilter.py`:

```python
import re

from community_intel.models import NormalizedItem

MIN_CHARS = 15

_KNOWN_BOTS = {"automoderator"}
_STRUCTURAL_NOISE = re.compile(
    r"^\s*(closed\s+via\s+#?\w+|merged\s+via\s+#?\w+|duplicate\s+of\s+#\d+)\s*$",
    re.IGNORECASE,
)
# Strip markdown, mentions and emoji so "👍" and "+1" collapse to nothing.
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_prefilter.py -v`
Expected: 12 passed

- [ ] **Step 5: Commit**

```bash
git add src/community_intel/prefilter.py tests/test_prefilter.py
git commit -m "feat: add ingest prefilter with self-loop guard"
```

---

### Task 5: GitHub adapter

**Files:**
- Create: `src/community_intel/adapters/__init__.py`
- Create: `src/community_intel/adapters/github.py`
- Test: `tests/test_adapter_github.py`

**Interfaces:**
- Consumes: `NormalizedItem`
- Produces: `from_webhook(event_type: str, payload: dict) -> NormalizedItem | None`
  returning `None` for event types and actions we do not ingest.
  Handles `issues`, `issue_comment`, `discussion`, `discussion_comment`.

- [ ] **Step 1: Write the failing test**

Create `tests/test_adapter_github.py`:

```python
from community_intel.adapters.github import from_webhook


def issue_payload(**over):
    p = {
        "action": "opened",
        "issue": {
            "id": 500, "number": 7, "title": "Crash on startup",
            "body": "It crashes when I launch it on 2.1.",
            "html_url": "https://github.com/acme/widget/issues/7",
            "created_at": "2026-09-01T12:00:00Z",
            "user": {"login": "octocat", "email": "leak@example.invalid"},
        },
        "repository": {"full_name": "acme/widget"},
        "sender": {"id": 42},
    }
    p.update(over)
    return p


def comment_payload():
    return {
        "action": "created",
        "issue": {"id": 500, "number": 7},
        "comment": {
            "id": 900, "body": "Same here, also on 2.1.",
            "html_url": "https://github.com/acme/widget/issues/7#issuecomment-900",
            "created_at": "2026-09-02T09:00:00Z",
            "user": {"login": "hubot"},
        },
        "repository": {"full_name": "acme/widget"},
        "sender": {"id": 43},
    }


def test_issue_maps_to_normalized_item():
    item = from_webhook("issues", issue_payload())
    assert item.item_id == "github#500"
    assert item.source_kind == "issue"
    assert item.channel == "acme/widget"
    assert item.title == "Crash on startup"
    assert item.parent_external_id is None
    assert item.created_at.year == 2026


def test_author_never_contains_email():
    item = from_webhook("issues", issue_payload())
    assert item.author == "octocat"
    assert "@" not in item.author


def test_comment_links_to_parent_issue_id():
    # This is what makes structural dedup work: the parent's external_id here
    # must equal the external_id the "issues" event stored for issue 7.
    item = from_webhook("issue_comment", comment_payload())
    assert item.item_id == "github#900"
    assert item.parent_external_id == "500"


def test_raw_payload_is_retained():
    item = from_webhook("issues", issue_payload())
    assert item.raw["sender"]["id"] == 42


def test_edited_and_deleted_actions_are_ignored():
    assert from_webhook("issues", issue_payload(action="deleted")) is None
    assert from_webhook("issues", issue_payload(action="labeled")) is None


def test_unknown_event_type_returns_none():
    assert from_webhook("push", {}) is None


def test_discussion_and_discussion_comment():
    d = {
        "action": "created",
        "discussion": {
            "id": 700, "title": "Feature idea",
            "body": "Would love dark mode.",
            "html_url": "https://github.com/acme/widget/discussions/3",
            "created_at": "2026-09-03T08:00:00Z",
            "user": {"login": "ada"},
        },
        "repository": {"full_name": "acme/widget"},
        "sender": {"id": 44},
    }
    item = from_webhook("discussion", d)
    assert item.item_id == "github#700"
    assert item.source_kind == "discussion"

    dc = dict(d)
    dc["comment"] = {
        "id": 800, "body": "Agreed, dark mode please.",
        "html_url": "https://github.com/acme/widget/discussions/3#c800",
        "created_at": "2026-09-03T09:00:00Z",
        "user": {"login": "grace"},
    }
    citem = from_webhook("discussion_comment", dc)
    assert citem.item_id == "github#800"
    assert citem.parent_external_id == "700"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_adapter_github.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'community_intel.adapters'`

- [ ] **Step 3: Write the implementation**

Create `src/community_intel/adapters/__init__.py` as an empty file.

Create `src/community_intel/adapters/github.py`:

```python
from datetime import datetime, timezone
from typing import Any

from community_intel.models import NormalizedItem

# Only these actions carry new text. "labeled", "closed", "deleted" and friends
# would otherwise re-ingest content we already have.
_ACCEPTED_ACTIONS = {"opened", "created"}


def _parse_ts(value: str) -> datetime:
    # GitHub sends RFC3339 with a trailing Z, which fromisoformat rejects
    # before 3.11 and accepts after; normalise explicitly for clarity.
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_adapter_github.py -v`
Expected: 8 passed

- [ ] **Step 5: Commit**

```bash
git add src/community_intel/adapters/ tests/test_adapter_github.py
git commit -m "feat: add GitHub webhook adapter for issues and discussions"
```

---

### Task 6: Webhook handler with signature verification

**Files:**
- Create: `src/community_intel/handlers/__init__.py`
- Create: `src/community_intel/handlers/webhook.py`
- Test: `tests/test_handler_webhook.py`

**Interfaces:**
- Consumes: `from_webhook`, `put_item_if_new`, `enqueue`, `rejection_reason`,
  `get_settings`
- Produces: `verify_signature(raw_body: bytes, header: str | None, secret: str) -> bool`
  and `handler(event: dict, context) -> dict` returning an API Gateway proxy
  response with `statusCode` 200, 401 or 400.

- [ ] **Step 1: Write the failing test**

Create `tests/test_handler_webhook.py`:

```python
import base64
import hashlib
import hmac
import json

import pytest

SECRET = "s3cret"


def sign(body: bytes, secret: str = SECRET) -> str:
    return "sha256=" + hmac.new(
        secret.encode(), body, hashlib.sha256
    ).hexdigest()


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


def test_handler_rejects_bad_signature(monkeypatch):
    monkeypatch.setenv("CIE_TABLE_NAME", "t")
    monkeypatch.setenv("CIE_QUEUE_URL", "u")
    monkeypatch.setenv("CIE_GITHUB_WEBHOOK_SECRET", SECRET)
    from community_intel.config import get_settings
    get_settings.cache_clear()

    from community_intel.handlers import webhook

    resp = webhook.handler(
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
    get_settings.cache_clear()


def test_handler_verifies_against_raw_base64_body(monkeypatch):
    # API Gateway may deliver the body base64-encoded. The HMAC must be
    # computed over the decoded raw bytes, never over a re-serialised dict.
    monkeypatch.setenv("CIE_TABLE_NAME", "t")
    monkeypatch.setenv("CIE_QUEUE_URL", "u")
    monkeypatch.setenv("CIE_GITHUB_WEBHOOK_SECRET", SECRET)
    monkeypatch.setenv("CIE_GITHUB_BOT_ID", "42")
    from community_intel.config import get_settings
    get_settings.cache_clear()

    from community_intel.handlers import webhook

    stored, queued = [], []
    monkeypatch.setattr(
        webhook, "put_item_if_new", lambda i: (stored.append(i), True)[1]
    )
    monkeypatch.setattr(webhook, "enqueue", lambda i: queued.append(i))

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

    resp = webhook.handler(
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
    get_settings.cache_clear()


def test_handler_stores_but_does_not_enqueue_rejected_item(monkeypatch):
    monkeypatch.setenv("CIE_TABLE_NAME", "t")
    monkeypatch.setenv("CIE_QUEUE_URL", "u")
    monkeypatch.setenv("CIE_GITHUB_WEBHOOK_SECRET", SECRET)
    monkeypatch.setenv("CIE_GITHUB_BOT_ID", "42")
    from community_intel.config import get_settings
    get_settings.cache_clear()

    from community_intel.handlers import webhook

    stored, queued = [], []
    monkeypatch.setattr(
        webhook, "put_item_if_new", lambda i: (stored.append(i), True)[1]
    )
    monkeypatch.setattr(webhook, "enqueue", lambda i: queued.append(i))

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

    resp = webhook.handler(
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
    get_settings.cache_clear()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_handler_webhook.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'community_intel.handlers'`

- [ ] **Step 3: Write the implementation**

Create `src/community_intel/handlers/__init__.py` as an empty file.

Create `src/community_intel/handlers/webhook.py`:

```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_handler_webhook.py -v`
Expected: 9 passed

- [ ] **Step 5: Commit**

```bash
git add src/community_intel/handlers/ tests/test_handler_webhook.py
git commit -m "feat: add GitHub webhook handler with HMAC verification"
```

---

### Task 7: Reddit adapter and poll handler

**Files:**
- Create: `src/community_intel/adapters/reddit.py`
- Create: `src/community_intel/handlers/reddit_poll.py`
- Test: `tests/test_adapter_reddit.py`

**Interfaces:**
- Consumes: `NormalizedItem`, `put_item_if_new`, `enqueue`, `rejection_reason`
- Produces: `from_submission(sub) -> NormalizedItem`,
  `from_comment(comment) -> NormalizedItem`,
  `read_cursor(subreddit: str) -> str | None`,
  `write_cursor(subreddit: str, fullname: str) -> None`,
  `handler(event: dict, context) -> dict` returning `{"ingested": int}`.

- [ ] **Step 1: Write the failing test**

Create `tests/test_adapter_reddit.py`:

```python
from types import SimpleNamespace

from community_intel.adapters.reddit import from_comment, from_submission


def fake_submission():
    return SimpleNamespace(
        name="t3_abc123",
        title="App crashes on 2.1",
        selftext="Every launch segfaults after upgrading.",
        permalink="/r/widget/comments/abc123/app_crashes/",
        created_utc=1788000000.0,
        author=SimpleNamespace(name="ada"),
        subreddit=SimpleNamespace(display_name="widget"),
    )


def fake_comment():
    return SimpleNamespace(
        name="t1_def456",
        body="Same here on 2.1, reproducible every time.",
        permalink="/r/widget/comments/abc123/app_crashes/def456/",
        created_utc=1788003600.0,
        author=SimpleNamespace(name="grace"),
        subreddit=SimpleNamespace(display_name="widget"),
        link_id="t3_abc123",
    )


def test_submission_maps_with_fullname_as_external_id():
    item = from_submission(fake_submission())
    assert item.item_id == "reddit#t3_abc123"
    assert item.source_kind == "submission"
    assert item.channel == "r/widget"
    assert item.parent_external_id is None
    assert item.url.startswith("https://www.reddit.com/")


def test_comment_links_to_parent_submission():
    item = from_comment(fake_comment())
    assert item.item_id == "reddit#t1_def456"
    assert item.parent_external_id == "t3_abc123"


def test_deleted_author_does_not_crash():
    sub = fake_submission()
    sub.author = None
    assert from_submission(sub).author == "[deleted]"


def test_created_at_comes_from_platform_epoch():
    item = from_submission(fake_submission())
    assert item.created_at.year == 2026
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_adapter_reddit.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'community_intel.adapters.reddit'`

- [ ] **Step 3: Write the adapter**

Create `src/community_intel/adapters/reddit.py`:

```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_adapter_reddit.py -v`
Expected: 4 passed

- [ ] **Step 5: Write the poll handler**

Create `src/community_intel/handlers/reddit_poll.py`:

```python
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


def _table():
    return boto3.resource("dynamodb").Table(get_settings().table_name)


def read_cursor(subreddit: str) -> str | None:
    resp = _table().get_item(Key={"pk": "CURSOR#reddit", "sk": subreddit})
    return (resp.get("Item") or {}).get("last_fullname")


def write_cursor(subreddit: str, fullname: str) -> None:
    _table().put_item(
        Item={"pk": "CURSOR#reddit", "sk": subreddit, "last_fullname": fullname}
    )


def _client() -> praw.Reddit:
    s = get_settings()
    return praw.Reddit(
        client_id=s.reddit_client_id,
        client_secret=s.reddit_client_secret,
        user_agent=s.reddit_user_agent,
    )


def _ingest(item) -> bool:
    """Store the item, enqueue it if it survives the prefilter."""
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
        cursor = read_cursor(name)
        newest: str | None = None

        # PRAW's stream.* is a polling loop with an in-memory seen-set that
        # cannot survive a Lambda invocation, so page explicitly instead.
        for listing, builder in (
            (sub.new(limit=PAGE), from_submission),
            (sub.comments(limit=PAGE), from_comment),
        ):
            for obj in listing:
                if newest is None:
                    newest = obj.name
                if cursor and obj.name == cursor:
                    break
                if _ingest(builder(obj)):
                    total += 1

        if newest:
            write_cursor(name, newest)

    log.info("reddit poll ingested=%d", total)
    return {"ingested": total}
```

- [ ] **Step 6: Run the full suite**

Run: `uv run pytest -v`
Expected: all tests pass

- [ ] **Step 7: Commit**

```bash
git add src/community_intel/adapters/reddit.py src/community_intel/handlers/reddit_poll.py tests/test_adapter_reddit.py
git commit -m "feat: add Reddit adapter and scheduled poll handler"
```

---

### Task 8: Docker-free Lambda packaging

**Files:**
- Create: `scripts/build_lambda.sh`
- Test: manual verification step below

**Interfaces:**
- Consumes: `pyproject.toml`
- Produces: a `build/lambda/` directory containing `community_intel/` and all
  runtime dependencies, suitable for `lambda_.Code.from_asset("build/lambda")`.

- [ ] **Step 1: Write the build script**

Create `scripts/build_lambda.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail

# Packages the Lambda bundle without Docker. uv's --python-platform fetches
# manylinux wheels for compiled dependencies (pydantic-core) so the bundle runs
# on Lambda regardless of the machine that built it.

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT="$ROOT/build/lambda"

rm -rf "$OUT"
mkdir -p "$OUT"

uv pip install \
  --target "$OUT" \
  --python-platform x86_64-manylinux2014 \
  --python-version 3.12 \
  --only-binary :all: \
  pydantic pydantic-settings praw

cp -R "$ROOT/src/community_intel" "$OUT/community_intel"

# boto3 is provided by the Lambda runtime; shipping it wastes ~10MB.
rm -rf "$OUT"/boto3 "$OUT"/botocore "$OUT"/*.dist-info

echo "built $OUT"
```

- [ ] **Step 2: Make it executable and run it**

```bash
chmod +x scripts/build_lambda.sh
./scripts/build_lambda.sh
```

Expected: `built /…/build/lambda`

- [ ] **Step 3: Verify the bundle structurally**

The bundle is cross-compiled for linux/x86_64, so it cannot be imported on a
macOS or ARM dev machine — `pydantic_core._pydantic_core` is a Linux `.so`.
Verification is structural instead, and `build_lambda.sh` runs it automatically:

```bash
python3 scripts/verify_bundle.py
```

Expected: all PASS — wheels target linux x86_64, dependencies present, boto3
excluded, both handler paths resolve, size under 250MB.

- [ ] **Step 4: Ignore build output**

```bash
echo "build/" >> .gitignore
```

- [ ] **Step 5: Commit**

```bash
git add scripts/build_lambda.sh .gitignore
git commit -m "build: add Docker-free Lambda packaging script"
```

---

### Task 9: CDK stack

**Files:**
- Create: `infra/__init__.py`
- Create: `infra/app.py`
- Create: `infra/stack.py`
- Create: `cdk.json`
- Test: `tests/test_stack.py`

**Interfaces:**
- Consumes: `build/lambda` from Task 8
- Produces: `IngestionStack` with a DynamoDB table, an SQS queue plus DLQ, two
  Lambda functions, an HTTP API, and an EventBridge rule.

- [ ] **Step 1: Write the failing test**

Create `tests/test_stack.py`:

```python
import aws_cdk as cdk
from aws_cdk.assertions import Match, Template

from infra.stack import IngestionStack


def template() -> Template:
    app = cdk.App()
    stack = IngestionStack(app, "TestStack")
    return Template.from_stack(stack)


def test_table_is_on_demand_with_composite_key():
    template().has_resource_properties(
        "AWS::DynamoDB::Table",
        {
            "BillingMode": "PAY_PER_REQUEST",
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
        },
    )


def test_queue_has_dead_letter_queue_after_three_attempts():
    template().has_resource_properties(
        "AWS::SQS::Queue",
        {"RedrivePolicy": Match.object_like({"maxReceiveCount": 3})},
    )


def test_two_lambda_functions_exist():
    template().resource_count_is("AWS::Lambda::Function", 2)


def test_reddit_poll_runs_every_two_minutes():
    template().has_resource_properties(
        "AWS::Events::Rule", {"ScheduleExpression": "rate(2 minutes)"}
    )


def test_no_wildcard_actions_in_policies():
    # The global constraint: IAM names explicit actions, never "*".
    policies = template().find_resources("AWS::IAM::Policy")
    for policy in policies.values():
        for stmt in policy["Properties"]["PolicyDocument"]["Statement"]:
            actions = stmt["Action"]
            actions = actions if isinstance(actions, list) else [actions]
            assert "*" not in actions
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_stack.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'infra'`

- [ ] **Step 3: Write the stack**

Create `infra/__init__.py` as an empty file.

Create `infra/stack.py`:

```python
from aws_cdk import Duration, RemovalPolicy, Stack
from aws_cdk import aws_apigatewayv2 as apigw
from aws_cdk import aws_apigatewayv2_integrations as integrations
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import aws_events as events
from aws_cdk import aws_events_targets as targets
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_sqs as sqs
from aws_cdk import aws_ssm as ssm
from constructs import Construct

BUNDLE = "build/lambda"


class IngestionStack(Stack):
    def __init__(self, scope: Construct, cid: str, **kwargs) -> None:
        super().__init__(scope, cid, **kwargs)

        table = dynamodb.Table(
            self, "Items",
            partition_key=dynamodb.Attribute(
                name="pk", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="sk", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.RETAIN,
        )

        dlq = sqs.Queue(self, "WorkDlq", retention_period=Duration.days(14))
        queue = sqs.Queue(
            self, "Work",
            visibility_timeout=Duration.seconds(180),
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=3, queue=dlq
            ),
        )

        secret = ssm.StringParameter.from_secure_string_parameter_attributes(
            self, "WebhookSecret",
            parameter_name="/community-intel/github-webhook-secret",
        )

        common_env = {
            "CIE_TABLE_NAME": table.table_name,
            "CIE_QUEUE_URL": queue.queue_url,
            "CIE_GITHUB_WEBHOOK_SECRET_PARAM": secret.parameter_name,
        }

        code = lambda_.Code.from_asset(BUNDLE)

        webhook = lambda_.Function(
            self, "Webhook",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="community_intel.handlers.webhook.handler",
            code=code,
            timeout=Duration.seconds(15),
            environment=common_env,
        )

        reddit = lambda_.Function(
            self, "RedditPoll",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="community_intel.handlers.reddit_poll.handler",
            code=code,
            timeout=Duration.seconds(120),
            environment=common_env,
        )

        # grant_* emits explicit action lists, never wildcards.
        for fn in (webhook, reddit):
            table.grant_read_write_data(fn)
            queue.grant_send_messages(fn)

        secret.grant_read(webhook)

        api = apigw.HttpApi(self, "Api")
        api.add_routes(
            path="/webhook/github",
            methods=[apigw.HttpMethod.POST],
            integration=integrations.HttpLambdaIntegration(
                "WebhookIntegration", webhook
            ),
        )

        events.Rule(
            self, "RedditSchedule",
            schedule=events.Schedule.rate(Duration.minutes(2)),
            targets=[targets.LambdaFunction(reddit)],
        )
```

Create `infra/app.py`:

```python
#!/usr/bin/env python3
import aws_cdk as cdk

from infra.stack import IngestionStack

app = cdk.App()
IngestionStack(app, "CommunityIntelIngestion")
app.synth()
```

Create `cdk.json`:

```json
{
  "app": "uv run python -m infra.app",
  "context": {
    "@aws-cdk/core:newStyleStackSynthesis": true
  }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_stack.py -v`
Expected: 5 passed

Note: the stack reads `build/lambda`, so run `./scripts/build_lambda.sh` first
if the directory does not exist.

- [ ] **Step 5: Verify synth succeeds**

Run: `uv run cdk synth`
Expected: CloudFormation YAML on stdout, no errors.

- [ ] **Step 6: Commit**

```bash
git add infra/ cdk.json tests/test_stack.py
git commit -m "feat: add CDK ingestion stack"
```

---

### Task 10: Deploy and verify end to end

**Files:**
- Modify: none (operational task)

**Interfaces:**
- Consumes: everything above
- Produces: a deployed stack and the first real items in DynamoDB

- [ ] **Step 1: Build and deploy**

```bash
./scripts/build_lambda.sh && uv run cdk bootstrap && uv run cdk deploy
```

Expected: stack creates, the HTTP API URL is printed as an output.

- [ ] **Step 2: Store the webhook secret in SSM**

The spec requires credentials in SSM SecureString, not plain environment
variables. Generate and store one:

```bash
SECRET=$(python3 -c "import secrets; print(secrets.token_hex(32))")
aws ssm put-parameter \
  --name /community-intel/github-webhook-secret \
  --type SecureString \
  --value "$SECRET" \
  --overwrite
echo "$SECRET"
```

The stack already sets `CIE_GITHUB_WEBHOOK_SECRET_PARAM` to that name and
grants the function `ssm:GetParameter` on it, so nothing else is needed. Keep
the printed value for Step 3.

- [ ] **Step 3: Register the webhook on a test repository**

In the repository settings, add a webhook pointing at
`<ApiUrl>/webhook/github`, content type `application/json`, with the secret from
Step 2. Subscribe to **Issues**, **Issue comments**, **Discussions**, and
**Discussion comments**.

- [ ] **Step 4: Verify a real delivery lands**

Open an issue on the test repository, then:

```bash
aws dynamodb scan --table-name <TableName> --max-items 5
```

Expected: one item with `pk` of the form `ITEM#github#<id>` and the issue body.

- [ ] **Step 5: Verify the prefilter gates the queue**

Comment `+1` on that issue, then re-scan. Expected: a second DynamoDB item
exists, but the SQS queue depth has not increased — the record is kept while the
pipeline invocation is not spent.

```bash
aws sqs get-queue-attributes --queue-url <QueueUrl> \
  --attribute-names ApproximateNumberOfMessages
```

- [ ] **Step 6: Leave it running**

The corpus for Plan 2 collects itself from here. Return in several days with
~200 items before labelling begins.

- [ ] **Step 7: Commit any operational notes**

```bash
git add -A
git commit -m "docs: record deployment verification steps"
```

---

## What Plan 2 covers

Written once the corpus exists, because its parameters are empirical:

- Labelling the corpus and the eval harness
- Classifier node and its few-shot examples, drawn from real labelled items
- Bedrock Titan embeddings and the S3 Vectors index
- Dedup resolver and the derivation of `MERGE_HIGH` from the labelled pairs
- Structurer node and the grounding eval
- GitHub App authentication, issue creation, and evidence comments
- Pipeline Lambda with `ReportBatchItemFailures` and reserved concurrency
- CloudWatch metrics, alarms, and the Bedrock budget alarm
