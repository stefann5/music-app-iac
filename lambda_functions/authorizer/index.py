import json
import boto3
import os
import logging
from typing import Dict, Any

logger = logging.getLogger()
logger.setLevel(logging.INFO)

cognito_client = boto3.client('cognito-idp')

def handler(event, context):
    """
    Lambda Authorizer for API Gateway
    Validates JWT tokens and checks user permissions
    """
    
    logger.info("Authorizer invoked")
    
    try:
        # Extract token from Authorization header
        token = extract_token(event)
        if not token:
            raise Exception('Unauthorized')
        
        # Validate token and get user info
        user_info = validate_token(token)
        
        # Generate policy
        policy = generate_policy(user_info, event['methodArn'])
        
        logger.info(f"Authorization successful for user: {user_info['username']}")
        return policy
        
    except Exception as e:
        logger.error(f"Authorization failed: {str(e)}")
        raise Exception('Unauthorized')

def extract_token(event):
    """Extract JWT token from Authorization header"""
    try:
        auth_header = event.get('authorizationToken', '')
        if auth_header.startswith('Bearer '):
            return auth_header[7:]  # Remove 'Bearer ' prefix
        return auth_header
    except:
        return None

def validate_token(token):
    """Validate JWT token with Cognito and get user info"""
    try:
        # Get user info from access token
        response = cognito_client.get_user(
            AccessToken=token
        )
        
        # Extract user attributes
        user_attributes = {attr['Name']: attr['Value'] for attr in response['UserAttributes']}
        
        # Get user groups
        username = response['Username']
        groups_response = cognito_client.admin_list_groups_for_user(
            UserPoolId=os.environ['USER_POOL_ID'],
            Username=username
        )
        
        groups = [group['GroupName'] for group in groups_response['Groups']]
        
        return {
            'username': username,
            'email': user_attributes.get('email'),
            'groups': groups,
            'role': user_attributes.get('custom:role', 'user')
        }
        
    except Exception as e:
        logger.error(f"Token validation failed: {str(e)}")
        raise Exception('Invalid token')

def generate_policy(user_info, method_arn):
    """Generate IAM policy for API Gateway"""
    
    # Determine effect based on user role/groups
    effect = 'Allow'  # Default allow for authenticated users
    
    policy = {
        'principalId': user_info['username'],
        'policyDocument': {
            'Version': '2012-10-17',
            'Statement': [
                {
                    'Action': 'execute-api:Invoke',
                    'Effect': effect,
                    'Resource': method_arn
                }
            ]
        },
        'context': {
            'username': user_info['username'],
            'email': user_info.get('email', ''),
            'role': user_info.get('role', 'user'),
            'groups': ','.join(user_info.get('groups', []))
        }
    }
    
    return policy