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
