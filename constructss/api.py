from aws_cdk import (
    aws_apigateway as apigateway,
    aws_lambda as _lambda
)
from constructs import Construct
from config import AppConfig

class ApiConstruct(Construct):
    """API Gateway infrastructure - extracted from existing code"""
    
    def __init__(
        self,
        scope: Construct,
        id: str,
        config: AppConfig,
        registration_function: _lambda.Function,
        login_function: _lambda.Function
    ):
        super().__init__(scope, id)
        
        self.config = config
        self.registration_function = registration_function
        self.login_function = login_function
        
        print(f"Creating API Gateway...")
        
        # Create API (your existing code)
        self.api = self._create_api_gateway()
    
    def _create_api_gateway(self) -> apigateway.RestApi:
        """Your existing _create_api_gateway method, mostly unchanged"""
        
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
        """Your existing _create_api_resources method, unchanged"""
        
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
        
        login_resource = auth_resource.add_resource('login')
        login_resource.add_method(
            'POST',
            apigateway.LambdaIntegration(self.login_function),
            method_responses=[
                apigateway.MethodResponse(status_code='200'),
                apigateway.MethodResponse(status_code='400'),
                apigateway.MethodResponse(status_code='401'),
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
        print("- POST /auth/login (implemented)")  # ADD THIS
        print("- /api/* (placeholder for future features)")
