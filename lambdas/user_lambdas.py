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
        users_table,
        artists_table
    ):
        super().__init__(scope, id)
        
        self.config = config
        self.user_pool = user_pool
        self.user_pool_client = user_pool_client
        self.users_table = users_table
        self.artists_table=artists_table
        
        print(f"Creating registration Lambda function...")
        
        # Create registration function (your existing code)
        self.registration_function = self._create_registration_function()
        
        print(f"Creating login Lambda function...")
        self.login_function = self._create_login_function()
        
        print(f"Creating refresh Lambda function...")  
        self.refresh_function = self._create_refresh_function()
        
        print(f"Creating authorizer Lambda function...") 
        self.authorizer_function = self._create_authorizer_function()  
        
        print(f"Creating create artist Lambda function...") 
        self.create_artist_function = self._create_create_artist_function()  
        
        print(f"Creating get artists Lambda function...")
        self.get_artists_function = self._create_get_artists_function()

        
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
    def _create_login_function(self) -> _lambda.Function:
        """Create Lambda function for user login"""
        
        return _lambda.Function(
            self,
            "LoginFunction",
            function_name=f"{self.config.app_name}-Login",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="index.handler",
            code=_lambda.Code.from_asset("lambda_functions/login"),
            timeout=Duration.seconds(self.config.lambda_timeout),
            memory_size=self.config.lambda_memory,
            tracing=_lambda.Tracing.ACTIVE if self.config.enable_x_ray_tracing else _lambda.Tracing.DISABLED,
            environment={
                'USER_POOL_ID': self.user_pool.user_pool_id,
                'USER_POOL_CLIENT_ID': self.user_pool_client.user_pool_client_id,
                'USERS_TABLE': self.users_table.table_name,
                'APP_NAME': self.config.app_name
            }
        )

    def _create_refresh_function(self) -> _lambda.Function:
        return _lambda.Function(
            self,
            "RefreshFunction",
            function_name=f"{self.config.app_name}-Refresh",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="index.handler",
            code=_lambda.Code.from_asset("lambda_functions/refresh"),
            timeout=Duration.seconds(self.config.lambda_timeout),
            memory_size=self.config.lambda_memory,
            environment={
                'USER_POOL_ID': self.user_pool.user_pool_id,
                'USER_POOL_CLIENT_ID': self.user_pool_client.user_pool_client_id,
                'APP_NAME': self.config.app_name
            }
        )
    
    def _create_authorizer_function(self) -> _lambda.Function:
        """Create Lambda authorizer function"""
        
        return _lambda.Function(
            self,
            "AuthorizerFunction", 
            function_name=f"{self.config.app_name}-Authorizer",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="index.handler",
            code=_lambda.Code.from_asset("lambda_functions/authorizer"),
            timeout=Duration.seconds(10),  # Authorizers should be fast
            memory_size=128,  # Minimal memory needed
            environment={
                'USER_POOL_ID': self.user_pool.user_pool_id,
                'APP_NAME': self.config.app_name
            }
        )

    def _create_create_artist_function(self) -> _lambda.Function:
        """Create Lambda function for creating artists"""
        
        return _lambda.Function(
            self,
            "CreateArtistFunction",
            function_name=f"{self.config.app_name}-CreateArtist", 
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="index.handler",
            code=_lambda.Code.from_asset("lambda_functions/create_artist"),
            timeout=Duration.seconds(self.config.lambda_timeout),
            memory_size=self.config.lambda_memory,
            tracing=_lambda.Tracing.ACTIVE if self.config.enable_x_ray_tracing else _lambda.Tracing.DISABLED,
            environment={
                'ARTISTS_TABLE': self.artists_table.table_name,
                'APP_NAME': self.config.app_name
            }
        )
    
    def _create_get_artists_function(self) -> _lambda.Function:
        """Create Lambda function for getting all artists"""
        
        return _lambda.Function(
            self,
            "GetArtistsFunction",
            function_name=f"{self.config.app_name}-GetArtists",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="index.handler",
            code=_lambda.Code.from_asset("lambda_functions/get_artists"),
            timeout=Duration.seconds(self.config.lambda_timeout),
            memory_size=self.config.lambda_memory,
            tracing=_lambda.Tracing.ACTIVE if self.config.enable_x_ray_tracing else _lambda.Tracing.DISABLED,
            environment={
                'ARTISTS_TABLE': self.artists_table.table_name,
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
        self.login_function.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    'cognito-idp:AdminInitiateAuth',
                    'cognito-idp:AdminGetUser'
                ],
                resources=[self.user_pool.user_pool_arn]
            )
        )
        
        self.refresh_function.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=['cognito-idp:AdminInitiateAuth'],
                resources=[self.user_pool.user_pool_arn]
            )
        )
        
        self.authorizer_function.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    'cognito-idp:GetUser',
                    'cognito-idp:AdminListGroupsForUser'
                ],
                resources=[self.user_pool.user_pool_arn]
            )
        )

        self.users_table.grant_read_write_data(self.login_function)
        
        self.users_table.grant_read_write_data(self.registration_function)
        
        self.artists_table.grant_read_write_data(self.create_artist_function)
        
        self.artists_table.grant_read_data(self.get_artists_function)
        
        print("Permissions granted successfully")
    
    