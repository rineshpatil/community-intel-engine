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


def test_table_is_retained_on_stack_deletion():
    # Stateful resource: losing the corpus to a cdk destroy would be fatal.
    template().has_resource("AWS::DynamoDB::Table", {"DeletionPolicy": "Retain"})


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
    assert policies, "expected at least one inline policy"
    for policy in policies.values():
        for stmt in policy["Properties"]["PolicyDocument"]["Statement"]:
            actions = stmt["Action"]
            actions = actions if isinstance(actions, list) else [actions]
            assert "*" not in actions


def test_outputs_needed_for_deployment_are_exported():
    outputs = template().find_outputs("*")
    assert {"ApiUrl", "TableName", "QueueUrl", "WebhookSecretParam"} <= set(outputs)


def test_webhook_can_read_only_its_own_ssm_parameter():
    policies = template().find_resources("AWS::IAM::Policy")
    ssm_statements = [
        stmt
        for policy in policies.values()
        for stmt in policy["Properties"]["PolicyDocument"]["Statement"]
        if "ssm:GetParameter" in (
            stmt["Action"] if isinstance(stmt["Action"], list) else [stmt["Action"]]
        )
    ]
    assert len(ssm_statements) == 1, "exactly one function may read the secret"


def test_dynamodb_grants_are_least_privilege():
    """Pin the exact DynamoDB actions each function gets.

    grant_read_write_data would also allow DeleteItem, Scan, BatchWriteItem and
    stream reads on the corpus table. Nothing in this codebase does any of
    those, so they must not be granted.
    """
    forbidden = {
        "dynamodb:DeleteItem", "dynamodb:Scan", "dynamodb:BatchWriteItem",
        "dynamodb:UpdateItem", "dynamodb:GetRecords",
    }
    granted = set()
    for policy in template().find_resources("AWS::IAM::Policy").values():
        for stmt in policy["Properties"]["PolicyDocument"]["Statement"]:
            actions = stmt["Action"]
            granted.update(actions if isinstance(actions, list) else [actions])

    assert granted & forbidden == set(), f"over-granted: {granted & forbidden}"
    assert "dynamodb:PutItem" in granted
    assert "dynamodb:GetItem" in granted    # reddit cursor reads


def test_every_billable_resource_carries_the_owner_tag():
    """Owner=rinesh_code must reach everything the budget filter can see.

    Untaggable types (inline IAM::Policy, Lambda::Permission, ApiGatewayV2
    Route/Integration, CDK::Metadata) have no Tags property in CloudFormation
    and incur no cost, so they are excluded rather than asserted on.
    """
    billable = [
        "AWS::DynamoDB::Table",
        "AWS::SQS::Queue",
        "AWS::Lambda::Function",
        "AWS::ApiGatewayV2::Api",
        "AWS::ApiGatewayV2::Stage",
        "AWS::Events::Rule",
    ]
    tpl = template()
    for rtype in billable:
        found = tpl.find_resources(rtype)
        assert found, f"no {rtype} in template"
        for lid, res in found.items():
            tags = res.get("Properties", {}).get("Tags")
            if isinstance(tags, dict):
                present = tags.get("Owner") == "rinesh_code"
            else:
                present = any(
                    t.get("Key") == "Owner" and t.get("Value") == "rinesh_code"
                    for t in (tags or [])
                )
            assert present, f"{rtype} {lid} is missing Owner=rinesh_code"
