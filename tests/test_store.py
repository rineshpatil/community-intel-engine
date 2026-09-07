import json
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
