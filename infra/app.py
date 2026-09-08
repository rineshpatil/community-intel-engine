#!/usr/bin/env python3
import os

import aws_cdk as cdk

from infra.stack import IngestionStack

app = cdk.App()

IngestionStack(
    app,
    "CommunityIntelIngestion",
    env=cdk.Environment(
        account=os.getenv("CDK_DEFAULT_ACCOUNT"),
        region=os.getenv("CDK_DEFAULT_REGION"),
    ),
)
app.synth()
