import json
import boto3
import os
import logging
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

cognito_client = boto3.client('cognito-idp')
dynamodb = boto3.resource('dynamodb')

def handler(event, context):
    """
    User Login Handler
    Implements requirement 1.2: Prijava na sistem
    """
    
    logger.info("Login request received")
    
    try:
        if not event.get('body'):
            return create_error_response(400, "Request body is required")
        
        body = json.loads(event['body'])
        
        # Validate input
        if not body.get('username') or not body.get('password'):
            return create_error_response(400, "Username and password are required")
        
        # Authenticate with Cognito
        try:
            response = cognito_client.admin_initiate_auth(
                UserPoolId=os.environ['USER_POOL_ID'],
                ClientId=os.environ['USER_POOL_CLIENT_ID'],
                AuthFlow='ADMIN_NO_SRP_AUTH',
                AuthParameters={
                    'USERNAME': body['username'],
                    'PASSWORD': body['password']
                }
            )
            
            # Update last login in DynamoDB
            update_last_login(body['username'])
            
            # Get user info
            user_info = get_user_info(body['username'])
            
            return create_success_response(200, {
                'message': 'Login successful',
                'tokens': {
                    'accessToken': response['AuthenticationResult']['AccessToken'],
                    'idToken': response['AuthenticationResult']['IdToken'],
                    'refreshToken': response['AuthenticationResult']['RefreshToken']
                },
                'user': user_info
            })
            
        except cognito_client.exceptions.NotAuthorizedException:
            return create_error_response(401, "Invalid username or password")
        except cognito_client.exceptions.UserNotFoundException:
            return create_error_response(401, "Invalid username or password")
        
    except Exception as e:
        logger.error(f"Login error: {str(e)}")
        return create_error_response(500, "Internal server error")

def update_last_login(username):
    """Update user's last login timestamp"""
    try:
        table = dynamodb.Table(os.environ['USERS_TABLE'])
        table.update_item(
            Key={'username': username},
            UpdateExpression="SET lastLogin = :timestamp",
            ExpressionAttributeValues={
                ':timestamp': datetime.utcnow().isoformat()
            },
            ConditionExpression="attribute_exists(username)"
        )
    except Exception as e:
        logger.warning(f"Could not update last login: {str(e)}")

def get_user_info(username):
    """Get user information from DynamoDB"""
    try:
        table = dynamodb.Table(os.environ['USERS_TABLE'])
        response = table.query(
            IndexName='username-index',
            KeyConditionExpression='username = :username',
            ExpressionAttributeValues={':username': username}
        )
        
        if response['Items']:
            user = response['Items'][0]
            return {
                'userId': user.get('userId'),
                'username': user.get('username'),
                'email': user.get('email'),
                'firstName': user.get('firstName'),
                'lastName': user.get('lastName'),
                'role': user.get('role', 'user')
            }
    except Exception as e:
        logger.error(f"Error getting user info: {str(e)}")
    
    return None

def create_success_response(status_code, data):
    return {
        'statusCode': status_code,
        'headers': get_cors_headers(),
        'body': json.dumps(data, default=str)
    }

def create_error_response(status_code, message):
    return {
        'statusCode': status_code,
        'headers': get_cors_headers(),
        'body': json.dumps({'error': message})
    }

def get_cors_headers():
    return {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type,Authorization,X-Requested-With',
        'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',
        'Content-Type': 'application/json'
    }
