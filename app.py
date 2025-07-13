# app.py
#!/usr/bin/env python3
import os
import aws_cdk as cdk
from music_app_cdk.registration_stack import RegistrationStack
from music_app_cdk.config import get_registration_config

app = cdk.App()

# Get stage from context or environment
stage = app.node.try_get_context("stage") or os.environ.get("STAGE", "dev")
config = get_registration_config(stage)

# Create registration stack
registration_stack = RegistrationStack(
    app,
    f"MusicApp-Registration-{stage.title()}",
    config=config,
    env=cdk.Environment(
        account=os.getenv('CDK_DEFAULT_ACCOUNT'),
        region=os.getenv('CDK_DEFAULT_REGION')
    )
)

# Add tags
cdk.Tags.of(registration_stack).add("Environment", stage)
cdk.Tags.of(registration_stack).add("Project", "MusicApp")
cdk.Tags.of(registration_stack).add("Component", "Registration")

app.synth()