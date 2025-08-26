from aws_cdk import (
    Stack,
    CfnOutput
)
from constructs import Construct
from config import AppConfig
from constructss.auth import AuthConstruct
from constructss.database import DatabaseConstruct
from constructss.api import ApiConstruct
from lambdas.user_lambdas import UserLambdas  # ← NEW IMPORT
from constructss.s3 import S3Construct

class MusicAppStack(Stack):
    
    def __init__(self, scope: Construct, construct_id: str, config: AppConfig, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        
        self.config = config
        
        print(f"Creating Music App infrastructure...")
        
        # Step 1: Create authentication system (Cognito)
        auth = AuthConstruct(self, "Auth", config)
        
        # Step 2: Create database tables (DynamoDB)
        database = DatabaseConstruct(self, "Database", config)
        s3 = S3Construct(self, "S3", config)
        
        # Step 3: Create Lambda functions
        user_lambdas = UserLambdas(  # ← CHANGED FROM ComputeConstruct
            self,
            "UserLambdas",  # ← CHANGED ID
            config,
            auth.user_pool,
            auth.user_pool_client,
            database.users_table,
            database.artists_table,
            database.ratings_table,
            database.subscriptions_table,
            database.music_content_table,
            s3.music_bucket
            database.notifications_table
        )
        
        # Step 4: Create API Gateway
        api = ApiConstruct(
            self,
            "Api",
            config,
            user_lambdas.registration_function,
            user_lambdas.login_function,
            user_lambdas.refresh_function,
            user_lambdas.authorizer_function, 
            user_lambdas.create_artist_function,
            user_lambdas.get_artists_function,
            user_lambdas.create_rating_function,
            user_lambdas.get_subscriptions_function,
            user_lambdas.create_subscription_function,
            user_lambdas.delete_subscription_function,
            user_lambdas.get_ratings_function,
            user_lambdas.get_music_content_function,
            user_lambdas.create_music_content_function,
            user_lambdas.update_music_content_function,
            user_lambdas.delete_music_content_function
            user_lambdas.notify_subscribers_function,
            user_lambdas.get_notifications_function
        )
        
        # Step 5: Create outputs (no changes needed here)
        self._create_outputs(auth, database, api, user_lambdas)
        
        print(f"Music App stack created successfully")
    
    def _create_outputs(self, auth, database, api, user_lambdas):  # ← Added user_lambdas parameter (optional)
        """Your existing outputs remain the same"""
        
        CfnOutput(
            self,
            "ApiUrl",
            value=api.api.url,
            description="Music App API Gateway URL",
            export_name=f"{self.config.app_name}-ApiUrl"
        )
        
        # ... rest of outputs unchanged