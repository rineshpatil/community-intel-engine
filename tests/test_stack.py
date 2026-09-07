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
