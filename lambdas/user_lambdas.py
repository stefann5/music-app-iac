from aws_cdk import (
    aws_lambda as _lambda,
    aws_iam as iam,
    Duration
)
from constructs import Construct
from config import AppConfig

class UserLambdas(Construct):
    """Lambda functions for user management - enhanced with album support and discover functionality"""
    
    def __init__(
        self,
        scope: Construct,
        id: str,
        config: AppConfig,
        user_pool,
        user_pool_client,
        users_table,
        artists_table,
        ratings_table,
        subscriptions_table,
        music_content_table,
        music_bucket,
        notifications_table,
        albums_table  # NEW: Albums table
    ):
        super().__init__(scope, id)
        
        self.config = config
        self.user_pool = user_pool
        self.user_pool_client = user_pool_client
        self.users_table = users_table
        self.ratings_table = ratings_table
        self.artists_table=artists_table
        self.subscriptions_table = subscriptions_table
        self.music_content_table = music_content_table
        self.music_bucket = music_bucket
        self.notifications_table = notifications_table
        self.albums_table = albums_table  # NEW: Store albums table reference
        
        print(f"Creating registration Lambda function...")
        self.registration_function = self._create_registration_function()
        
        print(f"Creating login Lambda function...")
        self.login_function = self._create_login_function()
        
        print(f"Creating refresh Lambda function...")  
        self.refresh_function = self._create_refresh_function()
        
        print(f"Creating authorizer Lambda function...") 
        self.authorizer_function = self._create_authorizer_function()  
        
        print(f"Creating create artist Lambda function...") 
        self.create_artist_function = self._create_create_artist_function()  

        print(f"Creating create rating Lambda function...") 
        self.create_rating_function = self._create_create_rating_function()  
        
        print(f"Creating get artists Lambda function...")
        self.get_artists_function = self._create_get_artists_function()

        print(f"Creating get subscriptions Lambda function...")
        self.get_subscriptions_function = self._create_get_subscriptions_function()

        print(f"Creating create subscriptions Lambda function...")
        self.create_subscription_function = self._create_subscription_function()

        print(f"Creating delete subscriptions Lambda function...")
        self.delete_subscription_function = self._create_delete_subscription_function()

        print(f"Creating get ratings Lambda function...")
        self.get_ratings_function = self._create_get_ratings_function()

        print(f"Creating create music content Lambda function...")
        self.create_music_content_function = self._create_create_music_content_function()

        print(f"Creating update music content Lambda function...")
        self.update_music_content_function = self._create_update_music_content_function()

        print(f"Creating get music content Lambda function...")
        self.get_music_content_function = self._create_get_music_content_function()

        print(f"Creating delete music content Lambda function...")
        self.delete_music_content_function = self._create_delete_music_content_function()

        print(f"Creating notifications Lambda function...")
        self.notify_subscribers_function = self._create_notify_subscribers_function()

        print(f"Creating get notifications Lambda function...")
        self.get_notifications_function = self._create_get_notifications_function()

        # NEW: Album management functions
        print(f"Creating create album Lambda function...")
        self.create_album_function = self._create_create_album_function()

        print(f"Creating get albums Lambda function...")
        self.get_albums_function = self._create_get_albums_function()

        # Enhanced discover functionality for albums and content
        print(f"Creating discover Lambda function...")
        self.discover_function = self._create_discover_function()

        # Grant permissions (includes new album functions)
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
    
    def _create_get_notifications_function(self) -> _lambda.Function:
        """Create Lambda function for getting all notifications"""
        
        return _lambda.Function(
            self,
            "GetNotificationsFunction",
            function_name=f"{self.config.app_name}-GetNotifications",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="index.handler",
            code=_lambda.Code.from_asset("lambda_functions/get_notifications"),
            timeout=Duration.seconds(self.config.lambda_timeout),
            memory_size=self.config.lambda_memory,
            tracing=_lambda.Tracing.ACTIVE if self.config.enable_x_ray_tracing else _lambda.Tracing.DISABLED,
            environment={
                'NOTIFICATIONS_TABLE': self.notifications_table.table_name,
                'APP_NAME': self.config.app_name
            }
        )
    
    
    def _create_get_subscriptions_function(self) -> _lambda.Function:
        """Create Lambda function for getting all subscriptions"""
        
        return _lambda.Function(
            self,
            "GetSubscriptionsFunction",
            function_name=f"{self.config.app_name}-GetSubscriptions",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="index.handler",
            code=_lambda.Code.from_asset("lambda_functions/get_subscriptions"),
            timeout=Duration.seconds(self.config.lambda_timeout),
            memory_size=self.config.lambda_memory,
            tracing=_lambda.Tracing.ACTIVE if self.config.enable_x_ray_tracing else _lambda.Tracing.DISABLED,
            environment={
                'SUBSCRIPTIONS_TABLE': self.subscriptions_table.table_name,
                'APP_NAME': self.config.app_name
            }
        )

    def _create_get_ratings_function(self) -> _lambda.Function:
        """Create Lambda function for getting all ratings"""
        
        return _lambda.Function(
            self,
            "GetRatingsFunction",
            function_name=f"{self.config.app_name}-GetRatings",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="index.handler",
            code=_lambda.Code.from_asset("lambda_functions/get_ratings"),
            timeout=Duration.seconds(self.config.lambda_timeout),
            memory_size=self.config.lambda_memory,
            tracing=_lambda.Tracing.ACTIVE if self.config.enable_x_ray_tracing else _lambda.Tracing.DISABLED,
            environment={
                'RATINGS_TABLE': self.ratings_table.table_name,
                'APP_NAME': self.config.app_name
            }
        )

    def _create_create_rating_function(self) -> _lambda.Function:
        """Create Lambda function for creating ratings"""
        
        return _lambda.Function(
            self,
            "CreateRatingFunction",
            function_name=f"{self.config.app_name}-CreateRating", 
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="index.handler",
            code=_lambda.Code.from_asset("lambda_functions/create_rating"),
            timeout=Duration.seconds(self.config.lambda_timeout),
            memory_size=self.config.lambda_memory,
            tracing=_lambda.Tracing.ACTIVE if self.config.enable_x_ray_tracing else _lambda.Tracing.DISABLED,
            environment={
                'RATINGS_TABLE': self.ratings_table.table_name,
                'APP_NAME': self.config.app_name
            }
        )
    
    def _create_subscription_function(self) -> _lambda.Function:
        """Create Lambda function for creating subscription"""
        
        return _lambda.Function(
            self,
            "CreateSubscriptionFunction",
            function_name=f"{self.config.app_name}-CreateSubscription", 
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="index.handler",
            code=_lambda.Code.from_asset("lambda_functions/create_subscription"),
            timeout=Duration.seconds(self.config.lambda_timeout),
            memory_size=self.config.lambda_memory,
            tracing=_lambda.Tracing.ACTIVE if self.config.enable_x_ray_tracing else _lambda.Tracing.DISABLED,
            environment={
                'SUBSCRIPTIONS_TABLE': self.subscriptions_table.table_name,
                'APP_NAME': self.config.app_name
            }
        )

    def _create_notify_subscribers_function(self) -> _lambda.Function:
        """Create Lambda function for creating subscription"""
        
        return _lambda.Function(
            self,
            "NotifySubscribersFunction",
            function_name=f"{self.config.app_name}-NotifySubscribers", 
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="index.handler",
            code=_lambda.Code.from_asset("lambda_functions/notify_subscribers"),
            timeout=Duration.seconds(self.config.lambda_timeout),
            memory_size=self.config.lambda_memory,
            tracing=_lambda.Tracing.ACTIVE if self.config.enable_x_ray_tracing else _lambda.Tracing.DISABLED,
            environment={
                'NOTIFICATIONS_TABLE': self.notifications_table.table_name,
                'APP_NAME': self.config.app_name
            }
        )

    def _create_delete_subscription_function(self) -> _lambda.Function:
        """Create Lambda function for deleting subscription"""
        
        return _lambda.Function(
            self,
            "DeleteSubscriptionFunction",
            function_name=f"{self.config.app_name}-DeleteSubscription", 
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="index.handler",  
            code=_lambda.Code.from_asset("lambda_functions/delete_subscription"),
            timeout=Duration.seconds(self.config.lambda_timeout),
            memory_size=self.config.lambda_memory,
            tracing=_lambda.Tracing.ACTIVE if self.config.enable_x_ray_tracing else _lambda.Tracing.DISABLED,
            environment={
                'SUBSCRIPTIONS_TABLE': self.subscriptions_table.table_name,
                'APP_NAME': self.config.app_name
            }
        )
    
    def _create_create_music_content_function(self) -> _lambda.Function:
        return _lambda.Function(
            self,
            "CreateMusicContentFunction",
            function_name=f"{self.config.app_name}-CreateMusicContent",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="index.handler",
            code=_lambda.Code.from_asset("lambda_functions/create_music_content"),
            timeout=Duration.seconds(self.config.lambda_timeout),
            memory_size=self.config.lambda_memory,
            tracing=_lambda.Tracing.ACTIVE if self.config.enable_x_ray_tracing else _lambda.Tracing.DISABLED,
            environment={
                'MUSIC_CONTENT_TABLE': self.music_content_table.table_name,
                'MUSIC_CONTENT_BUCKET': self.config.music_bucket_name,
                'MAX_FILE_SIZE': str(self.config.max_file_size),
                'ALLOWED_FILE_TYPES': ','.join(self.config.allowed_file_types),
                'ALLOWED_IMAGE_TYPES': ','.join(self.config.allowed_image_types),
                'MAX_IMAGE_SIZE': str(self.config.max_image_size),
                'ARTISTS_TABLE': self.artists_table.table_name,  # For updating artist metadata
                'ALBUMS_TABLE': self.albums_table.table_name     # NEW: For album relationship updates
            }
        )

    def _create_update_music_content_function(self) -> _lambda.Function:
        return _lambda.Function(
            self,
            "UpdateMusicContentFunction",
            function_name=f"{self.config.app_name}-UpdateMusicContent",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="index.handler",
            code=_lambda.Code.from_asset("lambda_functions/update_music_content"),
            timeout=Duration.seconds(self.config.lambda_timeout),
            memory_size=self.config.lambda_memory,
            tracing=_lambda.Tracing.ACTIVE if self.config.enable_x_ray_tracing else _lambda.Tracing.DISABLED,
            environment={
                'MUSIC_CONTENT_TABLE': self.music_content_table.table_name,
                'MUSIC_CONTENT_BUCKET': self.config.music_bucket_name,
                'MAX_FILE_SIZE': str(self.config.max_file_size),
                'ALLOWED_FILE_TYPES': ','.join(self.config.allowed_file_types),
                'ALLOWED_IMAGE_TYPES': ','.join(self.config.allowed_image_types),
                'MAX_IMAGE_SIZE': str(self.config.max_image_size)
            }
        )

    def _create_get_music_content_function(self) -> _lambda.Function:
        return _lambda.Function(
            self,
            "GetMusicContentFunction",
            function_name=f"{self.config.app_name}-GetMusicContent",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="index.handler",
            code=_lambda.Code.from_asset("lambda_functions/get_music_content"),
            timeout=Duration.seconds(self.config.lambda_timeout),
            memory_size=self.config.lambda_memory,
            tracing=_lambda.Tracing.ACTIVE if self.config.enable_x_ray_tracing else _lambda.Tracing.DISABLED,
            environment={
                'MUSIC_CONTENT_TABLE': self.music_content_table.table_name,
                'MUSIC_CONTENT_BUCKET': self.config.music_bucket_name,
                'APP_NAME': self.config.app_name
            }
        )

    def _create_delete_music_content_function(self) -> _lambda.Function:
        return _lambda.Function(
            self,
            "DeleteMusicContentFunction",
            function_name=f"{self.config.app_name}-DeleteMusicContent",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="index.handler",
            code=_lambda.Code.from_asset("lambda_functions/delete_music_content"),
            timeout=Duration.seconds(self.config.lambda_timeout),
            memory_size=self.config.lambda_memory,
            tracing=_lambda.Tracing.ACTIVE if self.config.enable_x_ray_tracing else _lambda.Tracing.DISABLED,
            environment={
                'MUSIC_CONTENT_TABLE': self.music_content_table.table_name,
                'MUSIC_CONTENT_BUCKET': self.config.music_bucket_name
            }
        )

    def _create_discover_function(self) -> _lambda.Function:
        """
        Create Lambda function for discover functionality with album support
        PERFORMANCE OPTIMIZED: Uses GSI queries for efficient genre-based filtering
        """
        return _lambda.Function(
            self,
            "DiscoverFunction",
            function_name=f"{self.config.app_name}-Discover",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="index.handler",
            code=_lambda.Code.from_asset("lambda_functions/discover"),
            timeout=Duration.seconds(self.config.lambda_timeout),
            memory_size=self.config.lambda_memory,
            tracing=_lambda.Tracing.ACTIVE if self.config.enable_x_ray_tracing else _lambda.Tracing.DISABLED,
            environment={
                'MUSIC_CONTENT_TABLE': self.music_content_table.table_name,
                'ARTISTS_TABLE': self.artists_table.table_name,
                'ALBUMS_TABLE': self.albums_table.table_name,  # NEW: Albums table for discover
                'APP_NAME': self.config.app_name
            }
        )

    def _create_create_album_function(self) -> _lambda.Function:
        """
        NEW: Create Lambda function for album creation (admin only)
        """
        return _lambda.Function(
            self,
            "CreateAlbumFunction",
            function_name=f"{self.config.app_name}-CreateAlbum",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="index.handler",
            code=_lambda.Code.from_asset("lambda_functions/create_album"),
            timeout=Duration.seconds(self.config.lambda_timeout),
            memory_size=self.config.lambda_memory,
            tracing=_lambda.Tracing.ACTIVE if self.config.enable_x_ray_tracing else _lambda.Tracing.DISABLED,
            environment={
                'ALBUMS_TABLE': self.albums_table.table_name,
                'ARTISTS_TABLE': self.artists_table.table_name,  # For artist verification
                'APP_NAME': self.config.app_name
            }
        )

    def _create_get_albums_function(self) -> _lambda.Function:
        """
        NEW: Create Lambda function for album retrieval with filtering
        """
        return _lambda.Function(
            self,
            "GetAlbumsFunction", 
            function_name=f"{self.config.app_name}-GetAlbums",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="index.handler",
            code=_lambda.Code.from_asset("lambda_functions/get_albums"),
            timeout=Duration.seconds(self.config.lambda_timeout),
            memory_size=self.config.lambda_memory,
            tracing=_lambda.Tracing.ACTIVE if self.config.enable_x_ray_tracing else _lambda.Tracing.DISABLED,
            environment={
                'ALBUMS_TABLE': self.albums_table.table_name,
                'MUSIC_CONTENT_TABLE': self.music_content_table.table_name,  # For track listings
                'APP_NAME': self.config.app_name
            }
        )

    def _grant_permissions(self):
        """Enhanced permissions including discover and album functions"""
        
        print("Granting permissions...")
        
        # Existing Cognito permissions
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

        # DynamoDB permissions for existing functions
        self.users_table.grant_read_write_data(self.login_function)
        self.users_table.grant_read_write_data(self.registration_function)
        self.artists_table.grant_read_write_data(self.create_artist_function)
        self.ratings_table.grant_read_write_data(self.create_rating_function)
        self.subscriptions_table.grant_read_write_data(self.create_subscription_function)
        self.artists_table.grant_read_data(self.get_artists_function)
        self.subscriptions_table.grant_read_data(self.get_subscriptions_function)
        self.subscriptions_table.grant_read_write_data(self.delete_subscription_function)
        self.ratings_table.grant_read_data(self.get_ratings_function)
        self.music_content_table.grant_read_write_data(self.create_music_content_function)
        self.music_content_table.grant_read_write_data(self.update_music_content_function)
        self.music_content_table.grant_read_data(self.get_music_content_function)
        self.music_content_table.grant_read_write_data(self.delete_music_content_function)
        self.notifications_table.grant_read_write_data(self.notify_subscribers_function)
        self.notifications_table.grant_read_data(self.get_notifications_function)
        
        # S3 permissions
        self.music_bucket.grant_read_write(self.create_music_content_function)
        self.music_bucket.grant_read(self.get_music_content_function)
        self.music_bucket.grant_read_write(self.delete_music_content_function)
        self.music_bucket.grant_read_write(self.update_music_content_function)

        # Discover function permissions - read access to all content tables
        self.music_content_table.grant_read_data(self.discover_function)
        self.artists_table.grant_read_data(self.discover_function)
        self.albums_table.grant_read_data(self.discover_function)  # NEW: Albums read access
        
        # Create music content needs access to albums for relationship updates
        self.albums_table.grant_read_write_data(self.create_music_content_function)  # NEW: For album metadata updates
        
        # Allow create_music_content to update artist metadata
        self.artists_table.grant_read_write_data(self.create_music_content_function)

        # NEW: Album function permissions
        self.albums_table.grant_read_write_data(self.create_album_function)  # Create albums
        self.artists_table.grant_read_data(self.create_album_function)       # Verify artist exists
        self.artists_table.grant_read_write_data(self.create_album_function) # Update artist album count
        
        self.albums_table.grant_read_data(self.get_albums_function)          # Read albums
        self.music_content_table.grant_read_data(self.get_albums_function)   # Read tracks for album details

        print("Permissions granted successfully, including album and discover functionality")