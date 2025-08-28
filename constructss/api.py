from aws_cdk import (
    Duration,
    aws_apigateway as apigateway,
    aws_lambda as _lambda
)
from constructs import Construct
from config import AppConfig

class ApiConstruct(Construct):
    """API Gateway infrastructure - enhanced with discover endpoints for performance-optimized filtering"""
    
    def __init__(
        self,
        scope: Construct,
        id: str,
        config: AppConfig,
        registration_function: _lambda.Function,
        login_function: _lambda.Function,
        refresh_function: _lambda.Function,
        authorizer_function: _lambda.Function,  
        create_artist_function: _lambda.Function,
        get_artists_function: _lambda.Function,
        create_rating_function: _lambda.Function,
        get_subscriptions_function: _lambda.Function,
        create_subscription_function: _lambda.Function,
        delete_subscription_function: _lambda.Function,
        get_ratings_function: _lambda.Function,
        get_music_content_function: _lambda.Function,
        create_music_content_function: _lambda.Function,
        update_music_content_function: _lambda.Function,
        delete_music_content_function: _lambda.Function,
        notify_subscribers_function: _lambda.Function,
        get_notifications_function: _lambda.Function,
        is_rated_function: _lambda.Function,
        is_subscribed_function: _lambda.Function,
        get_feed_function: _lambda.Function,
        discover_function: _lambda.Function,  # Enhanced discover with album support
        create_album_function: _lambda.Function,  # NEW: Album creation
        get_albums_function: _lambda.Function     # NEW: Album retrieval
    ):
        super().__init__(scope, id)
        
        self.config = config
        self.registration_function = registration_function
        self.login_function = login_function
        self.refresh_function = refresh_function
        self.authorizer_function = authorizer_function  
        self.create_artist_function = create_artist_function  
        self.get_artists_function = get_artists_function
        self.create_rating_function = create_rating_function
        self.create_subscription_function = create_subscription_function
        self.get_subscriptions_function = get_subscriptions_function
        self.delete_subscription_function = delete_subscription_function
        self.get_ratings_function = get_ratings_function
        self.get_music_content_function = get_music_content_function
        self.create_music_content_function = create_music_content_function
        self.update_music_content_function = update_music_content_function
        self.delete_music_content_function = delete_music_content_function
        self.notify_subscribers_function = notify_subscribers_function
        self.get_notifications_function = get_notifications_function
        self.is_rated_function = is_rated_function
        self.is_subscribed_function = is_subscribed_function
        self.get_feed_function = get_feed_function
        self.discover_function = discover_function  # Enhanced discover with album support
        self.create_album_function = create_album_function  # NEW: Album creation
        self.get_albums_function = get_albums_function      # NEW: Album retrieval
        
        print(f"Creating API Gateway with discover endpoints...")
        
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
            
            binary_media_types=[
                'multipart/form-data',
                'audio/*',
                'image/*',
                'application/octet-stream'
            ],

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
        
        # Add Gateway Responses for CORS on errors
        self._add_cors_gateway_responses(api)
        
        # Create API structure
        self._create_api_resources(api)
        
        return api
    
    def _create_api_resources(self, api: apigateway.RestApi):
        """Enhanced API resources with discover endpoints"""
        
        # Create authorizer for protected endpoints
        authorizer = self._create_lambda_authorizer()
        
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
        
        refresh_resource = auth_resource.add_resource('refresh')
        refresh_resource.add_method(
            'POST',
            apigateway.LambdaIntegration(self.refresh_function),
            method_responses=[
                apigateway.MethodResponse(status_code='200'),
                apigateway.MethodResponse(status_code='400'),
                apigateway.MethodResponse(status_code='401'),
                apigateway.MethodResponse(status_code='500')
            ]
        )

        # Artists endpoints
        artists_resource = api.root.add_resource('artists')
        artists_resource.add_method(
            'POST',
            apigateway.LambdaIntegration(self.create_artist_function),
            authorizer=authorizer,
            method_responses=[
                apigateway.MethodResponse(status_code='201'),
                apigateway.MethodResponse(status_code='400'),
                apigateway.MethodResponse(status_code='403'),
                apigateway.MethodResponse(status_code='409'),
                apigateway.MethodResponse(status_code='500')
            ]
        )

        artists_resource.add_method(
            'GET',
            apigateway.LambdaIntegration(self.get_artists_function),
            authorizer=authorizer,
            method_responses=[
                apigateway.MethodResponse(status_code='200'),
                apigateway.MethodResponse(status_code='401'),
                apigateway.MethodResponse(status_code='500')
            ]
        )

        # Rating endpoints
        rating_resource = api.root.add_resource('rating')
        rating_resource.add_method(
            'POST',
            apigateway.LambdaIntegration(self.create_rating_function),
            authorizer=authorizer,
            method_responses=[
                apigateway.MethodResponse(status_code='201'),
                apigateway.MethodResponse(status_code='400'),
                apigateway.MethodResponse(status_code='403'),
                apigateway.MethodResponse(status_code='409'),
                apigateway.MethodResponse(status_code='500')
            ]
        )

        rating_resource.add_method(
            'GET',
            apigateway.LambdaIntegration(self.get_ratings_function),
            authorizer=authorizer,
            method_responses=[
                apigateway.MethodResponse(status_code='200'),
                apigateway.MethodResponse(status_code='401'),
                apigateway.MethodResponse(status_code='500')
            ]
        )
       
        is_rated_resource = rating_resource.add_resource('check')

        is_rated_resource.add_method(
            'GET',
            apigateway.LambdaIntegration(self.is_rated_function),
            authorizer=authorizer,
            method_responses=[
                apigateway.MethodResponse(status_code='200'),
                apigateway.MethodResponse(status_code='401'),
                apigateway.MethodResponse(status_code='500')
            ]
        )


        # Notification endpoints
        notification_resource = api.root.add_resource('notification')
        notification_resource.add_method(
            'POST',
            apigateway.LambdaIntegration(self.notify_subscribers_function),
            authorizer=authorizer,
            method_responses=[
                apigateway.MethodResponse(status_code='201'),
                apigateway.MethodResponse(status_code='400'),
                apigateway.MethodResponse(status_code='403'),
                apigateway.MethodResponse(status_code='409'),
                apigateway.MethodResponse(status_code='500')
            ]
        )

        notification_resource.add_method(
            'GET',
            apigateway.LambdaIntegration(self.get_notifications_function),
            authorizer=authorizer,
            method_responses=[
                apigateway.MethodResponse(status_code='200'),
                apigateway.MethodResponse(status_code='401'),
                apigateway.MethodResponse(status_code='500')
            ]
        )

        # Subscription endpoints
        subscription_resource = api.root.add_resource('subscription')
        subscription_resource.add_method(
            'POST',
            apigateway.LambdaIntegration(self.create_subscription_function),
            authorizer=authorizer,
            method_responses=[
                apigateway.MethodResponse(status_code='201'),
                apigateway.MethodResponse(status_code='400'),
                apigateway.MethodResponse(status_code='403'),
                apigateway.MethodResponse(status_code='409'),
                apigateway.MethodResponse(status_code='500')
            ]
        )

        subscription_resource.add_method(
            'GET',
            apigateway.LambdaIntegration(self.get_subscriptions_function),
            authorizer=authorizer,
            method_responses=[
                apigateway.MethodResponse(status_code='200'),
                apigateway.MethodResponse(status_code='401'),
                apigateway.MethodResponse(status_code='500')
            ]
        )

        is_subscribed_resource = subscription_resource.add_resource('check')

        is_subscribed_resource.add_method(
            'GET',
            apigateway.LambdaIntegration(self.is_subscribed_function),
            authorizer=authorizer,
            method_responses=[
                apigateway.MethodResponse(status_code='200'),
                apigateway.MethodResponse(status_code='401'),
                apigateway.MethodResponse(status_code='500')
            ]
        )


        subscription_id_resource = subscription_resource.add_resource('{subscriptionId}')
        subscription_id_resource.add_method(
            'DELETE',
            apigateway.LambdaIntegration(self.delete_subscription_function),
            authorizer=authorizer,
            method_responses=[
                apigateway.MethodResponse(status_code='200'),
                apigateway.MethodResponse(status_code='400'),
                apigateway.MethodResponse(status_code='403'),
                apigateway.MethodResponse(status_code='404'),
                apigateway.MethodResponse(status_code='500')
            ]
        )

        # Music content endpoints
        music_content_resource = api.root.add_resource('music-content')
        music_content_resource.add_method(
            'GET',
            apigateway.LambdaIntegration(self.get_music_content_function),
            authorizer=authorizer,
            method_responses=[
                apigateway.MethodResponse(status_code='200'),
                apigateway.MethodResponse(status_code='401'),
                apigateway.MethodResponse(status_code='500')
            ]
        )

        feed_resource = music_content_resource.add_resource('feed')
        feed_resource.add_method(
            'GET',
            apigateway.LambdaIntegration(self.get_feed_function),
            authorizer=authorizer,
            method_responses=[
                apigateway.MethodResponse(status_code='200'),
                apigateway.MethodResponse(status_code='401'),
                apigateway.MethodResponse(status_code='500')
            ]
        )


        #creating music content
        music_content_resource.add_method(
            'POST',
            apigateway.LambdaIntegration(self.create_music_content_function),
            authorizer=authorizer,
            method_responses=[
                apigateway.MethodResponse(status_code='201'),
                apigateway.MethodResponse(status_code='400'),
                apigateway.MethodResponse(status_code='403'),
                apigateway.MethodResponse(status_code='409'),
                apigateway.MethodResponse(status_code='500')
            ]
        )

        music_content_resource.add_method(
            'PUT',
            apigateway.LambdaIntegration(self.update_music_content_function),
            authorizer=authorizer,
            method_responses=[
                apigateway.MethodResponse(status_code='200'),
                apigateway.MethodResponse(status_code='400'),
                apigateway.MethodResponse(status_code='403'),
                apigateway.MethodResponse(status_code='404'),
                apigateway.MethodResponse(status_code='500')
            ]
        )

        music_content_resource.add_method(
            'DELETE',
            apigateway.LambdaIntegration(self.delete_music_content_function),
            authorizer=authorizer,
            method_responses=[
                apigateway.MethodResponse(status_code='200'),
                apigateway.MethodResponse(status_code='400'),
                apigateway.MethodResponse(status_code='403'),
                apigateway.MethodResponse(status_code='404'),
                apigateway.MethodResponse(status_code='500')
            ]
        )

        # NEW: Discover endpoints for performance-optimized content filtering
        discover_resource = api.root.add_resource('discover')
        
        # GET /discover/genres - List all available genres
        genres_resource = discover_resource.add_resource('genres')
        genres_resource.add_method(
            'GET',
            apigateway.LambdaIntegration(self.discover_function),
            authorizer=authorizer,
            method_responses=[
                apigateway.MethodResponse(status_code='200'),
                apigateway.MethodResponse(status_code='401'),
                apigateway.MethodResponse(status_code='500')
            ]
        )
        
        # GET /discover/content?genre=rock&artistId=123&albumId=456
        content_resource = discover_resource.add_resource('content') 
        content_resource.add_method(
            'GET',
            apigateway.LambdaIntegration(self.discover_function),
            authorizer=authorizer,
            method_responses=[
                apigateway.MethodResponse(status_code='200'),
                apigateway.MethodResponse(status_code='400'),
                apigateway.MethodResponse(status_code='401'),
                apigateway.MethodResponse(status_code='500')
            ]
        )
        
        # GET /discover/artists?genre=rock
        discover_artists_resource = discover_resource.add_resource('artists')
        discover_artists_resource.add_method(
            'GET',
            apigateway.LambdaIntegration(self.discover_function),
            authorizer=authorizer,
            method_responses=[
                apigateway.MethodResponse(status_code='200'),
                apigateway.MethodResponse(status_code='400'),
                apigateway.MethodResponse(status_code='401'),
                apigateway.MethodResponse(status_code='500')
            ]
        )
        
        # GET /discover/albums?genre=rock
        albums_resource = discover_resource.add_resource('albums')
        albums_resource.add_method(
            'GET',
            apigateway.LambdaIntegration(self.discover_function),
            authorizer=authorizer,
            method_responses=[
                apigateway.MethodResponse(status_code='200'),
                apigateway.MethodResponse(status_code='400'),
                apigateway.MethodResponse(status_code='401'),
                apigateway.MethodResponse(status_code='500')
            ]
        )
        
        # NEW: Album management endpoints
        albums_mgmt_resource = api.root.add_resource('albums')
        
        # POST /albums - Create album (admin only)
        albums_mgmt_resource.add_method(
            'POST',
            apigateway.LambdaIntegration(self.create_album_function),
            authorizer=authorizer,
            method_responses=[
                apigateway.MethodResponse(status_code='201'),
                apigateway.MethodResponse(status_code='400'),
                apigateway.MethodResponse(status_code='403'),
                apigateway.MethodResponse(status_code='409'),
                apigateway.MethodResponse(status_code='500')
            ]
        )
        
        # GET /albums - Get albums with filtering
        albums_mgmt_resource.add_method(
            'GET',
            apigateway.LambdaIntegration(self.get_albums_function),
            authorizer=authorizer,
            method_responses=[
                apigateway.MethodResponse(status_code='200'),
                apigateway.MethodResponse(status_code='400'),
                apigateway.MethodResponse(status_code='401'),
                apigateway.MethodResponse(status_code='500')
            ]
        )
        
        print("API endpoints created:")
        print("- POST /auth/register (public)")
        print("- POST /auth/login (public)") 
        print("- POST /auth/refresh (public)")
        print("- POST /rating (protected, all users)")
        print("- POST /subscription (protected, all users)")
        print("- POST /notification (protected, all users)")
        print("- POST /artists (protected, admin only)")
        print("- GET /artists (protected, all users)")
        print("- GET /subscription (protected, all users)")
        print("- GET /subscription/check (protected, all users)")
        print("- GET /notification (protected, all users)")
        print("- GET /rating (implemented)")
        print("- GET /rating/check (implemented)")
        print("- DELETE /subscription (protected, all users)")
        print("- GET /rating (protected, all users)")
        print("- DELETE /subscription/{subscriptionId} (protected, all users)")
        print("- GET /music-content (protected, all users)")
        print("- POST /music-content (protected, admin only)")
        print("- PUT /music-content (protected, admin only)")
        print("- DELETE /music-content (protected, admin only)")
        print("")
        print("Album management endpoints:")
        print("- POST /albums (protected, admin only) - Create albums")
        print("- GET /albums?artistId=X&genre=X (protected, all users) - Get albums with filtering")
        print("")
        print("Discover endpoints (protected, all users):")
        print("- GET /discover/genres - List all available genres with counts (includes albums)")
        print("- GET /discover/content?genre=X - Get content by genre (chronological)")
        print("- GET /discover/artists?genre=X - Get artists by genre")
        print("- GET /discover/albums?genre=X - Get albums by genre")
        print("- Support for query parameters: genre, artistId, albumId, sortBy, limit, lastKey")
    
    def _create_lambda_authorizer(self) -> apigateway.TokenAuthorizer:
        """Create Lambda authorizer for protected endpoints"""
        
        return apigateway.TokenAuthorizer(
            self,
            "LambdaAuthorizer",
            handler=self.authorizer_function,
            identity_source="method.request.header.Authorization",
            results_cache_ttl=Duration.minutes(5)
        )

    def _add_cors_gateway_responses(self, api: apigateway.RestApi):
        """Add Gateway Responses to handle CORS on error responses"""
        
        cors_headers = {
            'Access-Control-Allow-Origin': "'*'",
            'Access-Control-Allow-Headers': "'Content-Type,Authorization,X-Requested-With'",
            'Access-Control-Allow-Methods': "'GET,POST,PUT,DELETE,OPTIONS'"
        }
        
        # Add gateway responses for common error codes
        try:
            api.add_gateway_response(
                "CorsGatewayResponse401",
                type=apigateway.ResponseType.UNAUTHORIZED,
                response_headers=cors_headers
            )
            
            api.add_gateway_response(
                "CorsGatewayResponse403", 
                type=apigateway.ResponseType.ACCESS_DENIED,
                response_headers=cors_headers
            )
            
            api.add_gateway_response(
                "CorsGatewayResponse404",
                type=apigateway.ResponseType.NOT_FOUND,
                response_headers=cors_headers
            )
            
            api.add_gateway_response(
                "CorsGatewayResponse4xx",
                type=apigateway.ResponseType.DEFAULT_4XX,
                response_headers=cors_headers
            )
            
            api.add_gateway_response(
                "CorsGatewayResponse5xx",
                type=apigateway.ResponseType.DEFAULT_5XX,
                response_headers=cors_headers
            )
            
            print("CORS Gateway Responses added successfully")
            
        except Exception as e:
            print(f"Warning: Could not add some gateway responses: {e}")