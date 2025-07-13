# music_app_cdk/music_app_stack.py
from aws_cdk import (
    Stack,
    aws_cognito as cognito,
    aws_dynamodb as dynamodb,
    aws_lambda as _lambda,
    aws_apigateway as apigateway,
    aws_iam as iam,
    Duration,
    RemovalPolicy,
    CfnOutput
)
from constructs import Construct
from .config import AppConfig

class MusicAppStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, config: AppConfig, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        
        self.config = config
        
        print(f"Creating Music App infrastructure...")
        
        # Step 1: Create authentication system (Cognito)
        self.user_pool = self._create_user_pool()
        self.user_pool_client = self._create_user_pool_client()
        
        # Step 2: Create database tables (DynamoDB)
        self.users_table = self._create_users_table()
        
        # Step 3: Create Lambda functions
        self.registration_function = self._create_registration_function()
        
        # Step 4: Create API Gateway
        self.api = self._create_api_gateway()
        
        # Step 5: Grant permissions
        self._grant_permissions()
        
        # Step 6: Create outputs
        self._create_outputs()
        
        print(f"Music App stack created successfully")
    
    def _create_user_pool(self) -> cognito.UserPool:
        """Create Cognito User Pool for user authentication"""
        
        print(f"Creating Cognito User Pool...")
        
        # Password policy configuration
        password_policy = cognito.PasswordPolicy(
            min_length=self.config.password_min_length,
            require_lowercase=True,
            require_uppercase=True,
            require_digits=True,
            require_symbols=False  # Keep it simple for now
        )
        
        user_pool = cognito.UserPool(
            self,
            "UserPool",
            user_pool_name=f"{self.config.app_name}-UserPool",
            
            # Sign-in configuration
            sign_in_aliases=cognito.SignInAliases(
                email=True,
                username=False  # Use email as username
            ),
            
            # Auto-verify email
            auto_verify=cognito.AutoVerifiedAttrs(email=True),
            
            # Required attributes (per requirement 1.1)
            standard_attributes=cognito.StandardAttributes(
                email=cognito.StandardAttribute(required=True, mutable=True),
                given_name=cognito.StandardAttribute(required=True, mutable=True),
                family_name=cognito.StandardAttribute(required=True, mutable=True),
                birthdate=cognito.StandardAttribute(required=True, mutable=True),
                preferred_username=cognito.StandardAttribute(required=True, mutable=True)
            ),
            
            # Custom attributes for future features
            custom_attributes={
                'role': cognito.StringAttribute(mutable=True),
                'subscription_type': cognito.StringAttribute(mutable=True)
            },
            
            # Password policy
            password_policy=password_policy,
            
            # Account recovery
            account_recovery=cognito.AccountRecovery.EMAIL_ONLY,
            
            # Email configuration
            email=cognito.UserPoolEmail.with_cognito(),
            
            # Removal policy
            removal_policy=RemovalPolicy.DESTROY
        )
        
        # Create user groups
        self._create_user_groups(user_pool)
        
        return user_pool
    
    def _create_user_groups(self, user_pool: cognito.UserPool):
        """Create user groups for role-based access"""
        
        # Administrator group (for content management)
        admin_group = cognito.CfnUserPoolGroup(
            self,
            "AdminGroup",
            user_pool_id=user_pool.user_pool_id,
            group_name="administrators",
            description="Administrator group with content management access",
            precedence=1
        )
        
        # Regular users group
        user_group = cognito.CfnUserPoolGroup(
            self,
            "UserGroup",
            user_pool_id=user_pool.user_pool_id,
            group_name="users", 
            description="Regular users with standard access",
            precedence=2
        )
        
        print("User groups created: administrators, users")
    
    def _create_user_pool_client(self) -> cognito.UserPoolClient:
        """Create User Pool Client for application access"""
        
        return cognito.UserPoolClient(
            self,
            "UserPoolClient",
            user_pool=self.user_pool,
            user_pool_client_name=f"{self.config.app_name}-Client",
            
            # Auth flows
            auth_flows=cognito.AuthFlow(
                user_password=True,
                admin_user_password=True,
                user_srp=True
            ),
            
            # Token validity
            access_token_validity=Duration.hours(1),
            id_token_validity=Duration.hours(1),
            refresh_token_validity=Duration.days(30),
            
            # No secret for web applications
            generate_secret=False,
            
            # Identity providers
            supported_identity_providers=[
                cognito.UserPoolClientIdentityProvider.COGNITO
            ]
        )
    
    def _create_users_table(self) -> dynamodb.Table:
        """Create DynamoDB table for user profiles"""
        
        print(f"Creating Users table...")
        
        table = dynamodb.Table(
            self,
            "UsersTable",
            table_name=f"{self.config.app_name}-Users",
            partition_key=dynamodb.Attribute(
                name='userId',
                type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY
        )
        
        # Add Global Secondary Index for username lookup
        table.add_global_secondary_index(
            index_name='username-index',
            partition_key=dynamodb.Attribute(
                name='username',
                type=dynamodb.AttributeType.STRING
            ),
            projection_type=dynamodb.ProjectionType.ALL
        )
        
        # Add Global Secondary Index for email lookup
        table.add_global_secondary_index(
            index_name='email-index',
            partition_key=dynamodb.Attribute(
                name='email',
                type=dynamodb.AttributeType.STRING
            ),
            projection_type=dynamodb.ProjectionType.ALL
        )
        
        print("Users table created with username and email indexes")
        return table
    
    def _create_registration_function(self) -> _lambda.Function:
        """Create Lambda function for user registration"""
        
        print(f"Creating registration Lambda function...")
        
        return _lambda.Function(
            self,
            "RegistrationFunction",
            function_name=f"{self.config.app_name}-Registration",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="index.handler",
            code=_lambda.Code.from_asset("lambda_functions/registration"),
            
            # Function configuration
            timeout=Duration.seconds(self.config.lambda_timeout),
            memory_size=self.config.lambda_memory,
            tracing=_lambda.Tracing.ACTIVE if self.config.enable_x_ray_tracing else _lambda.Tracing.DISABLED,
            
            # Environment variables
            environment={
                'USER_POOL_ID': self.user_pool.user_pool_id,
                'USER_POOL_CLIENT_ID': self.user_pool_client.user_pool_client_id,
                'USERS_TABLE': self.users_table.table_name,
                'PASSWORD_MIN_LENGTH': str(self.config.password_min_length),
                'APP_NAME': self.config.app_name
            }
        )
    
    def _create_api_gateway(self) -> apigateway.RestApi:
        """Create API Gateway for the Music App"""
        
        print(f"Creating API Gateway...")
        
        # Create REST API
        api = apigateway.RestApi(
            self,
            "MusicAppApi",
            rest_api_name=f"{self.config.app_name}-API",
            description=f"REST API for {self.config.app_name}",
            
            # CORS configuration
            default_cors_preflight_options=apigateway.CorsOptions(
                allow_origins=self.config.cors_origins,
                allow_methods=['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
                allow_headers=['Content-Type', 'Authorization', 'X-Requested-With']
            ),
            
            # Deploy automatically
            deploy=True,
            deploy_options=apigateway.StageOptions(
                stage_name='api',
                throttling_rate_limit=self.config.api_throttle_rate,
                throttling_burst_limit=self.config.api_throttle_burst,
                data_trace_enabled=False,  # Disable to avoid CloudWatch role requirement
                logging_level=apigateway.MethodLoggingLevel.OFF  # Disable logging
            )
        )
        
        # Create API structure
        self._create_api_resources(api)
        
        return api
    
    def _create_api_resources(self, api: apigateway.RestApi):
        """Create API resources and endpoints"""
        
        # Auth endpoints (public)
        auth_resource = api.root.add_resource('auth')
        
        # Registration endpoint
        register_resource = auth_resource.add_resource('register')
        register_resource.add_method(
            'POST',
            apigateway.LambdaIntegration(self.registration_function),
            method_responses=[
                apigateway.MethodResponse(status_code='200'),
                apigateway.MethodResponse(status_code='400'),
                apigateway.MethodResponse(status_code='409'),
                apigateway.MethodResponse(status_code='500')
            ]
        )
        
        # Future API endpoints will be added here
        # Protected endpoints (will require authorization)
        api_resource = api.root.add_resource('api')
        
        # Placeholder resources for future features
        # These will be implemented when adding new functionality:
        # - artists_resource = api_resource.add_resource('artists')
        # - songs_resource = api_resource.add_resource('songs')  
        # - users_resource = api_resource.add_resource('users')
        # - subscriptions_resource = api_resource.add_resource('subscriptions')
        
        print("API endpoints created:")
        print("- POST /auth/register (implemented)")
        print("- /api/* (placeholder for future features)")
    
    def _grant_permissions(self):
        """Grant necessary permissions to Lambda functions"""
        
        print("Granting permissions...")
        
        # Cognito permissions for registration function
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
        
        # DynamoDB permissions
        self.users_table.grant_read_write_data(self.registration_function)
        
        print("Permissions granted successfully")
    
    def _create_outputs(self):
        """Create CloudFormation outputs for important values"""
        
        CfnOutput(
            self,
            "ApiUrl",
            value=self.api.url,
            description="Music App API Gateway URL",
            export_name=f"{self.config.app_name}-ApiUrl"
        )
        
        CfnOutput(
            self,
            "UserPoolId",
            value=self.user_pool.user_pool_id,
            description="Cognito User Pool ID",
            export_name=f"{self.config.app_name}-UserPoolId"
        )
        
        CfnOutput(
            self,
            "UserPoolClientId",
            value=self.user_pool_client.user_pool_client_id,
            description="Cognito User Pool Client ID",
            export_name=f"{self.config.app_name}-UserPoolClientId"
        )
        
        CfnOutput(
            self,
            "UsersTableName",
            value=self.users_table.table_name,
            description="Users DynamoDB Table Name",
            export_name=f"{self.config.app_name}-UsersTable"
        )
        
        CfnOutput(
            self,
            "RegistrationEndpoint",
            value=f"{self.api.url}auth/register",
            description="User Registration Endpoint",
            export_name=f"{self.config.app_name}-RegistrationEndpoint"
        )
        
        print("CloudFormation outputs created")