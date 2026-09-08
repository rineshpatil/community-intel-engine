#!/usr/bin/env python3
import os

import aws_cdk as cdk

from infra.stack import IngestionStack

OWNER = "rinesh_code"

app = cdk.App()

# Applied at app scope so every taggable resource in every stack inherits it.
# The budget filter on Owner=rinesh_code depends on this being present.
cdk.Tags.of(app).add("Owner", OWNER)

IngestionStack(
    app,
    "CommunityIntelIngestion",
    env=cdk.Environment(
        account=os.getenv("CDK_DEFAULT_ACCOUNT"),
        region=os.getenv("CDK_DEFAULT_REGION"),
    ),
)
app.synth()
