from aws_cdk import (
    aws_dynamodb as dynamodb,
    RemovalPolicy
)
from constructs import Construct
from config import AppConfig

class DatabaseConstruct(Construct):
    """Database infrastructure - enhanced with Albums table and performance optimizations for discover functionality"""
    
    def __init__(self, scope: Construct, id: str, config: AppConfig):
        super().__init__(scope, id)
        
        self.config = config
        
        print(f"Creating Users table...")
        self.users_table = self._create_users_table()
        
        print(f"Creating Artists table...")
        self.artists_table = self._create_artists_table()

        print(f"Creating Albums table...")
        self.albums_table = self._create_albums_table()

        print(f"Creating Ratings table...")
        self.ratings_table = self._create_ratings_table()

        print(f"Creating Subscriptions table...")
        self.subscriptions_table = self._create_subscriptions_table()

        print(f"Creating music content table with album relationships...")
        self.music_content_table = self._create_music_content_table()

        print(f"Creating Notifications table...")
        self.notifications_table = self._create_notifications_table()
        
        print(f"Creating Transcriptions table...")
        self.transcriptions_table = self._create_transcriptions_table()
        
    
    def _create_users_table(self) -> dynamodb.Table:
        """Your existing _create_users_table method, unchanged"""
        
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
        
        table.add_global_secondary_index(
            index_name='username-index',
            partition_key=dynamodb.Attribute(
                name='username',
                type=dynamodb.AttributeType.STRING
            ),
            projection_type=dynamodb.ProjectionType.ALL
        )
        
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
    
    def _create_artists_table(self) -> dynamodb.Table:
        """Enhanced Artists table with better genre filtering performance"""
        
        table = dynamodb.Table(
            self,
            "ArtistsTable", 
            table_name=f"{self.config.app_name}-Artists",
            partition_key=dynamodb.Attribute(
                name='artistId',
                type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY
        )
        
        table.add_global_secondary_index(
            index_name='name-index',
            partition_key=dynamodb.Attribute(
                name='name',
                type=dynamodb.AttributeType.STRING
            ),
            projection_type=dynamodb.ProjectionType.ALL
        )
        
        # PERFORMANCE OPTIMIZATION: Add GSI for primary genre filtering
        table.add_global_secondary_index(
            index_name='primaryGenre-index',
            partition_key=dynamodb.Attribute(
                name='primaryGenre',
                type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name='name',
                type=dynamodb.AttributeType.STRING
            ),
            projection_type=dynamodb.ProjectionType.ALL
        )
        
        print("Artists table created with name and primaryGenre indexes for optimal discover performance")
        return table
    
    def _create_albums_table(self) -> dynamodb.Table:
        """
        Albums table for first-class album support in discover functionality
        PERFORMANCE OPTIMIZATION: GSI indexes for efficient album filtering (simplified)
        """
        
        table = dynamodb.Table(
            self,
            "AlbumsTable",
            table_name=f"{self.config.app_name}-Albums",
            partition_key=dynamodb.Attribute(
                name='albumId',
                type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY
        )
        
        # GSI for album title search
        table.add_global_secondary_index(
            index_name='title-index',
            partition_key=dynamodb.Attribute(
                name='title',
                type=dynamodb.AttributeType.STRING
            ),
            projection_type=dynamodb.ProjectionType.ALL
        )
        
        # PERFORMANCE OPTIMIZATION 1: Genre-based album filtering
        table.add_global_secondary_index(
            index_name='genre-createdAt-index',
            partition_key=dynamodb.Attribute(
                name='genre',
                type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name='createdAt',
                type=dynamodb.AttributeType.STRING
            ),
            projection_type=dynamodb.ProjectionType.ALL
        )
        
        # PERFORMANCE OPTIMIZATION 2: Artist-based album filtering
        table.add_global_secondary_index(
            index_name='artistId-createdAt-index',
            partition_key=dynamodb.Attribute(
                name='artistId',
                type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name='createdAt',
                type=dynamodb.AttributeType.STRING
            ),
            projection_type=dynamodb.ProjectionType.ALL
        )
        
        # PERFORMANCE OPTIMIZATION 3: Genre + Artist combination for drill-down
        table.add_global_secondary_index(
            index_name='genre-artistId-index',
            partition_key=dynamodb.Attribute(
                name='genre',
                type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name='artistId',
                type=dynamodb.AttributeType.STRING
            ),
            projection_type=dynamodb.ProjectionType.ALL
        )
        
        print("Albums table created with optimized indexes for discover functionality:")
        print("- genre-createdAt-index: Fast genre filtering with chronological order")
        print("- artistId-createdAt-index: Efficient artist album queries")
        print("- genre-artistId-index: Genre + artist drill-down")
        
        return table
    
    def _create_ratings_table(self) -> dynamodb.Table:
        """Enhanced ratings table with album support"""
    
        table = dynamodb.Table(
            self,
            "RatingsTable", 
            table_name=f"{self.config.app_name}-Ratings",
            partition_key=dynamodb.Attribute(
                name='ratingId',
                type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY
        )
        
        # Existing indexes
        table.add_global_secondary_index(
            index_name='userId-index',
            partition_key=dynamodb.Attribute(
                name='userId',
                type=dynamodb.AttributeType.STRING
            ),
            projection_type=dynamodb.ProjectionType.ALL
        )
        
        table.add_global_secondary_index(
            index_name='songId-index',
            partition_key=dynamodb.Attribute(
                name='songId',
                type=dynamodb.AttributeType.STRING
            ),
            projection_type=dynamodb.ProjectionType.ALL
        )
        
        table.add_global_secondary_index(
            index_name='userId-songId-index',
            partition_key=dynamodb.Attribute(
                name='userId',
                type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name='songId',
                type=dynamodb.AttributeType.STRING
            ),
            projection_type=dynamodb.ProjectionType.ALL
        )
        
        # NEW: Album-based ratings
        table.add_global_secondary_index(
            index_name='albumId-index',
            partition_key=dynamodb.Attribute(
                name='albumId',
                type=dynamodb.AttributeType.STRING
            ),
            projection_type=dynamodb.ProjectionType.ALL
        )
        
        print("Ratings table created with userId, songId, albumId indexes")
        return table
    
    def _create_subscriptions_table(self) -> dynamodb.Table:
        """Enhanced subscriptions table with album subscription support"""
        
        table = dynamodb.Table(
            self,
            "SubscriptionsTable", 
            table_name=f"{self.config.app_name}-Subscriptions",
            partition_key=dynamodb.Attribute(
                name='subscriptionId',
                type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY
        )
        
        # Existing indexes
        table.add_global_secondary_index(
            index_name='userId-index',
            partition_key=dynamodb.Attribute(
                name='userId',
                type=dynamodb.AttributeType.STRING
            ),
            projection_type=dynamodb.ProjectionType.ALL
        )
        
        table.add_global_secondary_index(
            index_name='subscriptionType-targetId-index',
            partition_key=dynamodb.Attribute(
                name='subscriptionType',
                type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name='targetId',
                type=dynamodb.AttributeType.STRING
            ),
            projection_type=dynamodb.ProjectionType.ALL
        )

        
        print("Subscriptions table created with album subscription support")
        return table
    
    def _create_music_content_table(self) -> dynamodb.Table:
        """Enhanced MusicContent table with proper album relationships and performance optimizations"""
        
        table = dynamodb.Table(
            self,
            "MusicContentTable", 
            table_name=f"{self.config.app_name}-MusicContent",
            partition_key=dynamodb.Attribute(
                name='contentId',
                type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY
        )
        
        # Existing indexes
        table.add_global_secondary_index(
            index_name='title-index',
            partition_key=dynamodb.Attribute(
                name='title',
                type=dynamodb.AttributeType.STRING
            ),
            projection_type=dynamodb.ProjectionType.ALL
        )
        
        table.add_global_secondary_index(
            index_name='artistId-index',
            partition_key=dynamodb.Attribute(
                name='artistId',
                type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name='createdAt',
                type=dynamodb.AttributeType.STRING
            ),
            projection_type=dynamodb.ProjectionType.ALL
        )
        
        # ENHANCED: Album-based content queries
        table.add_global_secondary_index(
            index_name='albumId-trackNumber-index',
            partition_key=dynamodb.Attribute(
                name='albumId',
                type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name='trackNumber',
                type=dynamodb.AttributeType.NUMBER
            ),
            projection_type=dynamodb.ProjectionType.ALL
        )
        
        # Genre-based optimizations
        table.add_global_secondary_index(
            index_name='genre-createdAt-index',
            partition_key=dynamodb.Attribute(
                name='genre',
                type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name='createdAt',
                type=dynamodb.AttributeType.STRING
            ),
            projection_type=dynamodb.ProjectionType.ALL
        )
        
        table.add_global_secondary_index(
            index_name='genre-artistId-index',
            partition_key=dynamodb.Attribute(
                name='genre',
                type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name='artistId',
                type=dynamodb.AttributeType.STRING
            ),
            projection_type=dynamodb.ProjectionType.ALL
        )
        
        print("MusicContent table created with album relationships and optimized indexes:")
        print("- albumId-trackNumber-index: Album track listings in order")
        print("- Enhanced genre indexes for discover functionality")
        
        return table

    def _create_notifications_table(self) -> dynamodb.Table:
        """Notifications table with album notification support"""

        table = dynamodb.Table(
            self,
            "NotificationsTable",
            table_name=f"{self.config.app_name}-Notifications",
            partition_key=dynamodb.Attribute(
                name="notificationId",
                type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY
        )

        table.add_global_secondary_index(
            index_name="subscriber-index",
            partition_key=dynamodb.Attribute(
                name="subscriber",
                type=dynamodb.AttributeType.STRING
            ),
            projection_type=dynamodb.ProjectionType.ALL
        )

        table.add_global_secondary_index(
            index_name="contentId-index",
            partition_key=dynamodb.Attribute(
                name="contentId",
                type=dynamodb.AttributeType.STRING
            ),
            projection_type=dynamodb.ProjectionType.ALL
        )

        # NEW: Album-based notifications
        table.add_global_secondary_index(
            index_name="albumId-index",
            partition_key=dynamodb.Attribute(
                name="albumId",
                type=dynamodb.AttributeType.STRING
            ),
            projection_type=dynamodb.ProjectionType.ALL
        )

        print("Notifications table created with album notification support")
        return table
    
    def _create_transcriptions_table(self) -> dynamodb.Table:
        """Transcriptions table for song lyrics/text with contentId as primary key"""
        
        table = dynamodb.Table(
            self,
            "TranscriptionsTable",
            table_name=f"{self.config.app_name}-Transcriptions",
            partition_key=dynamodb.Attribute(
                name='contentId', 
                type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY
        )
        
        # GSI for status-based monitoring and cleanup
        table.add_global_secondary_index(
            index_name='status-createdAt-index',
            partition_key=dynamodb.Attribute(
                name='status',
                type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name='createdAt',
                type=dynamodb.AttributeType.STRING
            ),
            projection_type=dynamodb.ProjectionType.ALL
        )
        
        # GSI for job name lookups (for monitoring)
        table.add_global_secondary_index(
            index_name='jobName-index',
            partition_key=dynamodb.Attribute(
                name='jobName',
                type=dynamodb.AttributeType.STRING
            ),
            projection_type=dynamodb.ProjectionType.ALL
        )
        
        print("Transcriptions table created with contentId as primary key")
        return table