from aws_cdk import (
    aws_cognito as cognito,
    RemovalPolicy,
    Duration
)
from constructs import Construct
from config import AppConfig

class AuthConstruct(Construct):
    """Authentication infrastructure - updated to use username for login"""
    
    def __init__(self, scope: Construct, id: str, config: AppConfig):
        super().__init__(scope, id)
        
        self.config = config
        
        print(f"Creating Cognito User Pool...")
        
        # Create User Pool (updated for username login)
        self.user_pool = self._create_user_pool()
        
        # Create User Pool Client (your existing code)
        self.user_pool_client = self._create_user_pool_client()
        
        # Create user groups (your existing code)
        self._create_user_groups()
    
    def _create_user_pool(self) -> cognito.UserPool:
        """Updated to use username for sign-in instead of email"""
        
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
            
            # Sign-in configuration - UPDATED to use username
            sign_in_aliases=cognito.SignInAliases(
                email=False,
                username=True  # Use username for sign-in
            ),

            
            # Auto-verify email (but don't use it for login)
            auto_verify=cognito.AutoVerifiedAttrs(email=True),
            
            # Required attributes (per requirement 1.1)
            standard_attributes=cognito.StandardAttributes(
                email=cognito.StandardAttribute(required=True, mutable=True),
                given_name=cognito.StandardAttribute(required=True, mutable=True),
                family_name=cognito.StandardAttribute(required=True, mutable=True),
                birthdate=cognito.StandardAttribute(required=True, mutable=True)
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
        
        return user_pool
    
    def _create_user_groups(self):
        """Your existing _create_user_groups method, unchanged"""
        
        # Administrator group (for content management)
        admin_group = cognito.CfnUserPoolGroup(
            self,
            "AdminGroup",
            user_pool_id=self.user_pool.user_pool_id,
            group_name="administrators",
            description="Administrator group with content management access",
            precedence=1
        )
        
        # Regular users group
        user_group = cognito.CfnUserPoolGroup(
            self,
            "UserGroup",
            user_pool_id=self.user_pool.user_pool_id,
            group_name="users", 
            description="Regular users with standard access",
            precedence=2
        )
        
        print("User groups created: administrators, users")
    
    def _create_user_pool_client(self) -> cognito.UserPoolClient:
        """Your existing _create_user_pool_client method, unchanged"""
        
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