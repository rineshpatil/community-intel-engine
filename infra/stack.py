import pathlib

from aws_cdk import ArnFormat, CfnOutput, Duration, RemovalPolicy, Stack
from aws_cdk import aws_apigatewayv2 as apigw
from aws_cdk import aws_apigatewayv2_integrations as integrations
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import aws_events as events
from aws_cdk import aws_events_targets as targets
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_sqs as sqs
from constructs import Construct

# Absolute, so synth works regardless of the caller's working directory.
BUNDLE = str(pathlib.Path(__file__).resolve().parent.parent / "build" / "lambda")

WEBHOOK_SECRET_PARAM = "/community-intel/github-webhook-secret"


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
            # The corpus is irreplaceable; never let a destroy take it.
            removal_policy=RemovalPolicy.RETAIN,
        )

        dlq = sqs.Queue(self, "WorkDlq", retention_period=Duration.days(14))
        queue = sqs.Queue(
            self, "Work",
            visibility_timeout=Duration.seconds(180),
            dead_letter_queue=sqs.DeadLetterQueue(max_receive_count=3, queue=dlq),
        )

        common_env = {
            "CIE_TABLE_NAME": table.table_name,
            "CIE_QUEUE_URL": queue.queue_url,
            "CIE_GITHUB_WEBHOOK_SECRET_PARAM": WEBHOOK_SECRET_PARAM,
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

        # Scoped to the one parameter, not the whole /community-intel/ path.
        webhook.add_to_role_policy(
            iam.PolicyStatement(
                actions=["ssm:GetParameter"],
                resources=[
                    self.format_arn(
                        service="ssm",
                        resource="parameter",
                        resource_name=WEBHOOK_SECRET_PARAM.lstrip("/"),
                        arn_format=ArnFormat.SLASH_RESOURCE_NAME,
                    )
                ],
            )
        )

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

        # Task 10 needs these to register the webhook and inspect ingestion.
        CfnOutput(self, "ApiUrl", value=api.api_endpoint)
        CfnOutput(self, "TableName", value=table.table_name)
        CfnOutput(self, "QueueUrl", value=queue.queue_url)
        CfnOutput(self, "WebhookSecretParam", value=WEBHOOK_SECRET_PARAM)
