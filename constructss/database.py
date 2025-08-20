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