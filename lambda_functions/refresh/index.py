import boto3
import json
import os  # ← MISSING
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

cognito_client = boto3.client('cognito-idp')

def handler(event, context):
    logger.info("Token refresh request received")
    
    try:
        if not event.get('body'):
            return create_error_response(400, "Request body is required")
        
        body = json.loads(event['body'])
        
        if not body.get('refreshToken'):
            return create_error_response(400, "Refresh token is required")
        
        refresh_token = body['refreshToken']
        
        response = cognito_client.admin_initiate_auth(
            UserPoolId=os.environ['USER_POOL_ID'],
            ClientId=os.environ['USER_POOL_CLIENT_ID'],
            AuthFlow='REFRESH_TOKEN_AUTH',
            AuthParameters={
                'REFRESH_TOKEN': refresh_token
            }
        )
        
        auth_result = response['AuthenticationResult']
        
        return create_success_response(200, {
            'message': 'Tokens refreshed successfully',
            'accessToken': auth_result['AccessToken'],
            'idToken': auth_result['IdToken'],
            'refreshToken': refresh_token
        })
        
    except cognito_client.exceptions.NotAuthorizedException:
        return create_error_response(401, "Invalid or expired refresh token")
    except Exception as e:
        logger.error(f"Token refresh error: {str(e)}")
        return create_error_response(500, "Token refresh failed")

def create_success_response(status_code, data):
    return {
        'statusCode': status_code,
        'headers': get_cors_headers(),
        'body': json.dumps(data)
    }

def create_error_response(status_code, message):
    return {
        'statusCode': status_code,
        'headers': get_cors_headers(),
        'body': json.dumps({'error': message})
    }

def get_cors_headers():  # ← YOU WERE MISSING THIS
    return {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type,Authorization,X-Requested-With',
        'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',
        'Content-Type': 'application/json'
    }