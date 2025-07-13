import os
import aws_cdk as cdk
from music_app_cdk.music_app_stack import MusicAppStack
from music_app_cdk.config import get_app_config

app = cdk.App()

# Get configuration for the music app
config = get_app_config()

# Create the main music app stack
music_app_stack = MusicAppStack(
    app,
    "MusicAppStack",
    config=config,
    env=cdk.Environment(
        account=os.getenv('CDK_DEFAULT_ACCOUNT'),
        region=os.getenv('CDK_DEFAULT_REGION')
    )
)

# Add tags
cdk.Tags.of(music_app_stack).add("Project", "MusicApp")
cdk.Tags.of(music_app_stack).add("Component", "Backend")
cdk.Tags.of(music_app_stack).add("ManagedBy", "CDK")

app.synth()