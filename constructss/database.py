from aws_cdk import (
    aws_dynamodb as dynamodb,
    RemovalPolicy
)
from constructs import Construct
from config import AppConfig

class DatabaseConstruct(Construct):
    """Database infrastructure - extracted from existing code"""
    
    def __init__(self, scope: Construct, id: str, config: AppConfig):
        super().__init__(scope, id)
        
        self.config = config
        
        print(f"Creating Users table...")
        
        # Create users table (your existing code)
        self.users_table = self._create_users_table()
        
        print(f"Creating Artists table...")
        self.artists_table = self._create_artists_table()

        print(f"Creating Ratings table...")
        self.ratings_table = self._create_ratings_table()

        print(f"Creating Subscriptions table...")
        self.subscriptions_table = self._create_subscriptions_table()

        print(f"Creating music conntent table...")
        self.music_content_table = self._create_music_content_table()
    
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
    
    def _create_artists_table(self) -> dynamodb.Table:
        """Create Artists table for storing artist information"""
        
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
        
        # Add Global Secondary Index for name lookup
        table.add_global_secondary_index(
            index_name='name-index',
            partition_key=dynamodb.Attribute(
                name='name',
                type=dynamodb.AttributeType.STRING
            ),
            projection_type=dynamodb.ProjectionType.ALL
        )
        
        print("Artists table created with name index")
        return table
    
    def _create_ratings_table(self) -> dynamodb.Table:
    
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
        
        # Add Global Secondary Index for user lookup (da vidite sve ocene korisnika)
        table.add_global_secondary_index(
            index_name='userId-index',
            partition_key=dynamodb.Attribute(
                name='userId',
                type=dynamodb.AttributeType.STRING
            ),
            projection_type=dynamodb.ProjectionType.ALL
        )
        
        # Add Global Secondary Index for song lookup (da vidite sve ocene pesme)
        table.add_global_secondary_index(
            index_name='songId-index',
            partition_key=dynamodb.Attribute(
                name='songId',
                type=dynamodb.AttributeType.STRING
            ),
            projection_type=dynamodb.ProjectionType.ALL
        )
        
        # Add Global Secondary Index za kombinaciju userId i songId 
        # (da proverite da li je korisnik već ocenio pesmu)
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
        
        print("Ratings table created with userId, songId, and userId-songId indexes")
        return table
    
    def _create_subscriptions_table(self) -> dynamodb.Table:
        """Create Subscriptions table for user content subscriptions"""
        
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
        
        # GSI za pronalaženje svih pretplata korisnika
        table.add_global_secondary_index(
            index_name='userId-index',
            partition_key=dynamodb.Attribute(
                name='userId',
                type=dynamodb.AttributeType.STRING
            ),
            projection_type=dynamodb.ProjectionType.ALL
        )
        
        # GSI za pronalaženje pretplata po tipu i vrednosti
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
        
        # GSI za kombinaciju korisnika i tipa (provera duplikata)
        table.add_global_secondary_index(
            index_name='userId-subscriptionType-targetId-index',
            partition_key=dynamodb.Attribute(
                name='userId',
                type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name='compositeKey',  # userId#subscriptionType#targetId
                type=dynamodb.AttributeType.STRING
            ),
            projection_type=dynamodb.ProjectionType.ALL
        )
        
        print("Subscriptions table created with indexes")
        return table
    
    def _create_music_content_table(self) -> dynamodb.Table:
        """Create MusicContent table for storing music content information"""
        
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
        
        # Add Global Secondary Index for title lookup
        table.add_global_secondary_index(
            index_name='title-index',
            partition_key=dynamodb.Attribute(
                name='title',
                type=dynamodb.AttributeType.STRING
            ),
            projection_type=dynamodb.ProjectionType.ALL
        )
        
        # Add Global Secondary Index for artistId lookup
        table.add_global_secondary_index(
            index_name='artistId-index',
            partition_key=dynamodb.Attribute(
                name='artistId',
                type=dynamodb.AttributeType.STRING
            ),
            projection_type=dynamodb.ProjectionType.ALL
        )
        
        print("MusicContent table created with title and artistId indexes")
        return table