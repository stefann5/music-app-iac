from aws_cdk import (
    aws_lambda as _lambda,
    aws_iam as iam,
    Duration
)
from constructs import Construct
from config import AppConfig

class UserLambdas(Construct):
    """Lambda functions for user management"""
    
    def __init__(
        self,
        scope: Construct,
        id: str,
        config: AppConfig,
        user_pool,
        user_pool_client,
        users_table
    ):
        super().__init__(scope, id)
        
        self.config = config
        self.user_pool = user_pool
        self.user_pool_client = user_pool_client
        self.users_table = users_table
        
        print(f"Creating registration Lambda function...")
        
        # Create registration function (your existing code)
        self.registration_function = self._create_registration_function()
        
        # Grant permissions (your existing code)
        self._grant_permissions()
    
    def _create_registration_function(self) -> _lambda.Function:
        """Your existing _create_registration_function method"""
        
        return _lambda.Function(
            self,
            "RegistrationFunction",
            function_name=f"{self.config.app_name}-Registration",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="index.handler",
            code=_lambda.Code.from_asset("lambda_functions/registration"),
            timeout=Duration.seconds(self.config.lambda_timeout),
            memory_size=self.config.lambda_memory,
            tracing=_lambda.Tracing.ACTIVE if self.config.enable_x_ray_tracing else _lambda.Tracing.DISABLED,
            environment={
                'USER_POOL_ID': self.user_pool.user_pool_id,
                'USER_POOL_CLIENT_ID': self.user_pool_client.user_pool_client_id,
                'USERS_TABLE': self.users_table.table_name,
                'PASSWORD_MIN_LENGTH': str(self.config.password_min_length),
                'APP_NAME': self.config.app_name
            }
        )
    
    def _grant_permissions(self):
        """Your existing _grant_permissions method"""
        
        print("Granting permissions...")
        
        self.registration_function.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    'cognito-idp:AdminCreateUser',
                    'cognito-idp:AdminSetUserPassword',
                    'cognito-idp:AdminAddUserToGroup',
                    'cognito-idp:AdminGetUser'
                ],
                resources=[self.user_pool.user_pool_arn]
            )
        )
        
        self.users_table.grant_read_write_data(self.registration_function)
        
        print("Permissions granted successfully")