import aws_cdk as core
import aws_cdk.assertions as assertions

from config_cdk.config_cdk_stack import ConfigCdkStack

# example tests. To run these tests, uncomment this file along with the example
# resource in config_cdk/config_cdk_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = ConfigCdkStack(app, "config-cdk")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
