# music_app_cdk/registration_stack.py
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
from .config import RegistrationConfig

class RegistrationStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, config: RegistrationConfig, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        
        self.config = config
        
        print(f"Creating registration infrastructure for {config.stage} environment...")
        
        # Step 1: Create Cognito User Pool (imperative configuration)
        self.user_pool = self._create_user_pool()
        
        # Step 2: Create Cognito User Pool Client
        self.user_pool_client = self._create_user_pool_client()
        
        # Step 3: Create DynamoDB table for user profiles
        self.users_table = self._create_users_table()
        
        # Step 4: Create Lambda function for registration
        self.registration_function = self._create_registration_function()
        
        # Step 5: Create API Gateway
        self.api = self._create_api_gateway()
        
        # Step 6: Grant permissions (imperative permission assignment)
        self._grant_permissions()
        
        # Step 7: Create outputs
        self._create_outputs()
        
        print(f"Registration stack created successfully for {config.stage}")
    
    def _create_user_pool(self) -> cognito.UserPool:
        """Create Cognito User Pool with imperative configuration"""
        
        print(f"Creating Cognito User Pool with {self.config.password_min_length} char minimum password...")
        
        # Determine removal policy imperatively
        removal_policy = RemovalPolicy.DESTROY if self.config.stage == 'dev' else RemovalPolicy.RETAIN
        
        # Create password policy based on stage
        if self.config.stage == 'dev':
            password_policy = cognito.PasswordPolicy(
                min_length=self.config.password_min_length,
                require_lowercase=True,
                require_uppercase=True,
                require_digits=True,
                require_symbols=False  # Easier for development
            )
        else:  # production
            password_policy = cognito.PasswordPolicy(
                min_length=self.config.password_min_length,
                require_lowercase=True,
                require_uppercase=True,
                require_digits=True,
                require_symbols=True  # More secure for production
            )
        
        user_pool = cognito.UserPool(
            self,
            "UserPool",
            user_pool_name=f"{self.config.app_name}-{self.config.stage}-UserPool",
            
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
            
            # Custom attributes
            custom_attributes={
                'role': cognito.StringAttribute(mutable=True)
            },
            
            # Password policy (imperative decision based on stage)
            password_policy=password_policy,
            
            # Account recovery
            account_recovery=cognito.AccountRecovery.EMAIL_ONLY,
            
            # Email configuration
            email=cognito.UserPoolEmail.with_cognito(),
            
            # Removal policy (imperative decision)
            removal_policy=removal_policy
        )
        
        # Create user groups imperatively
        self._create_user_groups(user_pool)
        
        return user_pool
    
    def _create_user_groups(self, user_pool: cognito.UserPool):
        """Create user groups imperatively"""
        
        # Administrator group
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
        """Create User Pool Client with stage-specific configuration"""
        
        # Configure token validity based on stage (imperative decision)
        if self.config.stage == 'dev':
            access_token_validity = Duration.hours(2)  # Longer for development
            id_token_validity = Duration.hours(2)
            refresh_token_validity = Duration.days(30)
        else:  # production
            access_token_validity = Duration.hours(1)  # Shorter for security
            id_token_validity = Duration.hours(1)
            refresh_token_validity = Duration.days(7)
        
        return cognito.UserPoolClient(
            self,
            "UserPoolClient",
            user_pool=self.user_pool,
            user_pool_client_name=f"{self.config.app_name}-{self.config.stage}-Client",
            
            # Auth flows
            auth_flows=cognito.AuthFlow(
                user_password=True,
                admin_user_password=True,
                user_srp=True
            ),
            
            # Token validity (imperative configuration)
            access_token_validity=access_token_validity,
            id_token_validity=id_token_validity,
            refresh_token_validity=refresh_token_validity,
            
            # No secret for web applications
            generate_secret=False,
            
            # Security features (imperative decision based on stage)
            prevent_user_existence_errors=True if self.config.stage == 'prod' else False,
            
            # Identity providers
            supported_identity_providers=[
                cognito.UserPoolClientIdentityProvider.COGNITO
            ]
        )
    
    def _create_users_table(self) -> dynamodb.Table:
        """Create DynamoDB table for user profiles"""
        
        print(f"Creating Users table with {self.config.removal_policy} removal policy...")
        
        # Configure table based on stage (imperative configuration)
        if self.config.stage == 'dev':
            table_config = {
                'billing_mode': dynamodb.BillingMode.PAY_PER_REQUEST,
                'point_in_time_recovery': False,
                'removal_policy': RemovalPolicy.DESTROY
            }
        else:  # production
            table_config = {
                'billing_mode': dynamodb.BillingMode.PROVISIONED,
                'read_capacity': 5,
                'write_capacity': 5,
                'point_in_time_recovery': True,
                'removal_policy': RemovalPolicy.RETAIN
            }
        
        table = dynamodb.Table(
            self,
            "UsersTable",
            table_name=f"{self.config.app_name}-{self.config.stage}-Users",
            partition_key=dynamodb.Attribute(
                name='userId',
                type=dynamodb.AttributeType.STRING
            ),
            billing_mode=table_config['billing_mode'],
            point_in_time_recovery_specification=dynamodb.PointInTimeRecoverySpecification(
                point_in_time_recovery_enabled=table_config['point_in_time_recovery']
            ),
            removal_policy=table_config['removal_policy']
        )
        
        # Add provisioned throughput if needed (imperative decision)
        if table_config['billing_mode'] == dynamodb.BillingMode.PROVISIONED:
            table.add_property_override('ProvisionedThroughput', {
                'ReadCapacityUnits': table_config['read_capacity'],
                'WriteCapacityUnits': table_config['write_capacity']
            })
        
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
        """Create Lambda function for registration with imperative configuration"""
        
        print(f"Creating registration Lambda with {self.config.lambda_memory}MB memory...")
        
        # Configure function based on stage (imperative decision)
        function_config = {
            'timeout': Duration.seconds(self.config.lambda_timeout),
            'memory_size': self.config.lambda_memory,
            'tracing': _lambda.Tracing.ACTIVE if self.config.stage == 'prod' else _lambda.Tracing.DISABLED,
            'retry_attempts': 0 if self.config.stage == 'dev' else 2
        }
        
        return _lambda.Function(
            self,
            "RegistrationFunction",
            function_name=f"{self.config.app_name}-{self.config.stage}-Registration",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="index.handler",
            code=_lambda.Code.from_asset("lambda_functions/registration"),
            
            # Imperative configuration based on stage
            timeout=function_config['timeout'],
            memory_size=function_config['memory_size'],
            tracing=function_config['tracing'],
            retry_attempts=function_config['retry_attempts'],
            
            # Environment variables
            environment={
                'STAGE': self.config.stage,
                'USER_POOL_ID': self.user_pool.user_pool_id,
                'USER_POOL_CLIENT_ID': self.user_pool_client.user_pool_client_id,
                'USERS_TABLE': self.users_table.table_name,
                'PASSWORD_MIN_LENGTH': str(self.config.password_min_length)
            }
        )
    
    def _create_api_gateway(self) -> apigateway.RestApi:
        """Create API Gateway with imperative endpoint configuration"""
        
        print(f"Creating API Gateway with CORS origins: {self.config.cors_origins}")
        
        # Create REST API
        api = apigateway.RestApi(
            self,
            "RegistrationApi",
            rest_api_name=f"{self.config.app_name}-{self.config.stage}-Registration-API",
            description=f"Registration API for {self.config.app_name} {self.config.stage}",
            
            # CORS configuration (imperative based on stage)
            default_cors_preflight_options=apigateway.CorsOptions(
                allow_origins=self.config.cors_origins,
                allow_methods=['POST', 'OPTIONS'],
                allow_headers=['Content-Type', 'Authorization']
            ),
            
            # Deploy automatically
            deploy=True,
            deploy_options=apigateway.StageOptions(
                stage_name=self.config.stage,
                
                # Configure throttling based on stage (imperative decision)
                throttling_rate_limit=100 if self.config.stage == 'dev' else 1000,
                throttling_burst_limit=200 if self.config.stage == 'dev' else 2000,
                
                # Enable logging in production
                access_log_destination=self._create_api_log_group() if self.config.stage == 'prod' else None,
                data_trace_enabled=self.config.stage == 'dev'  # Debug logs only in dev
            )
        )
        
        # Create registration endpoint imperatively
        self._create_registration_endpoint(api)
        
        return api
    
    def _create_api_log_group(self):
        """Create CloudWatch log group for API Gateway (production only)"""
        from aws_cdk import aws_logs as logs
        
        return logs.LogGroup(
            self,
            "ApiLogGroup",
            log_group_name=f"/aws/apigateway/{self.config.app_name}-{self.config.stage}",
            removal_policy=RemovalPolicy.DESTROY if self.config.stage == 'dev' else RemovalPolicy.RETAIN
        )
    
    def _create_registration_endpoint(self, api: apigateway.RestApi):
        """Create registration endpoint imperatively"""
        
        # Create auth resource
        auth_resource = api.root.add_resource('auth')
        
        # Create register resource
        register_resource = auth_resource.add_resource('register')
        
        # Add POST method for registration (simplified)
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
        
        print("Registration endpoint created: POST /auth/register")
    
    def _grant_permissions(self):
        """Grant permissions to Lambda function imperatively"""
        
        print("Granting permissions to registration function...")
        
        # Cognito permissions
        self.registration_function.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    'cognito-idp:AdminCreateUser',
                    'cognito-idp:AdminSetUserPassword',
                    'cognito-idp:AdminAddUserToGroup'
                ],
                resources=[self.user_pool.user_pool_arn]
            )
        )
        
        # DynamoDB permissions
        self.users_table.grant_read_write_data(self.registration_function)
        
        # Grant additional permissions based on stage (imperative decision)
        if self.config.stage == 'prod':
            # More restrictive permissions in production
            self.registration_function.add_to_role_policy(
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=['logs:CreateLogGroup', 'logs:CreateLogStream', 'logs:PutLogEvents'],
                    resources=[f"arn:aws:logs:*:*:log-group:/aws/lambda/{self.registration_function.function_name}:*"]
                )
            )
        
        print("Permissions granted successfully")
    
    def _create_outputs(self):
        """Create CloudFormation outputs"""
        
        CfnOutput(
            self,
            "RegistrationApiUrl",
            value=self.api.url,
            description="Registration API Gateway URL",
            export_name=f"{self.config.app_name}-{self.config.stage}-Registration-ApiUrl"
        )
        
        CfnOutput(
            self,
            "UserPoolId",
            value=self.user_pool.user_pool_id,
            description="Cognito User Pool ID",
            export_name=f"{self.config.app_name}-{self.config.stage}-UserPoolId"
        )
        
        CfnOutput(
            self,
            "UserPoolClientId",
            value=self.user_pool_client.user_pool_client_id,
            description="Cognito User Pool Client ID",
            export_name=f"{self.config.app_name}-{self.config.stage}-UserPoolClientId"
        )
        
        CfnOutput(
            self,
            "UsersTableName",
            value=self.users_table.table_name,
            description="Users DynamoDB Table Name",
            export_name=f"{self.config.app_name}-{self.config.stage}-UsersTable"
        )
        
        print("CloudFormation outputs created")