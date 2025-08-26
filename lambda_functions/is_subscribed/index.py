import base64
import json
from xml.dom.minidom import Attr
import boto3
import os
from datetime import datetime
import logging
from boto3.dynamodb.conditions import Key 

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')

def handler(event, context):
    """
    Get All Ratings Handler
    Protected endpoint for retrieving all ratings
    """
    
    logger.info("Get all ratings request received")
    
    try:
        # Parse query parameters
        query_params = event.get('queryStringParameters') or {}
        subscriptionType = query_params.get('subscriptionType')
        target_id = query_params.get('target_id')
        target_name = query_params.get('target_name')

        request_context = event.get('requestContext', {})
        authorizer = request_context.get('authorizer', {})
        username = authorizer.get('username', {})
        
        # Get subscriptions from DynamoDB
        is_subscribed = has_subscribed(username, subscriptionType, target_id, target_name)
                
        response_data = {
            'is_subscribed': is_subscribed,
        }
        
        return create_success_response(200, response_data)
        
    except Exception as e:
        logger.error(f"Get ratings error: {str(e)}")
        return create_error_response(500, "Internal server error")

def has_subscribed(username: str, subscriptionType: str, target_id: str = None, target_name: str = None) -> bool:
    """
    Check if a user is already subscribed.
    - target_type: 'ARTIST' or 'GENRE'
    - For ARTIST, provide target_id
    - For GENRE, provide target_name
    Returns True if subscription exists, False otherwise.
    """
    try:
        table = dynamodb.Table(os.environ['SUBSCRIPTIONS_TABLE'])

        if subscriptionType == 'ARTIST':
            if not target_id:
                raise ValueError("target_id must be provided for ARTIST subscription")
            response = table.scan(
                FilterExpression='#username = :username AND #targetId = :targetId AND #subscriptionType = :subscriptionType',
                ExpressionAttributeNames={
                    '#username': 'username',
                    '#targetId': 'targetId',
                    '#subscriptionType': 'subscriptionType'
                },
                ExpressionAttributeValues={
                    ':username': username,
                    ':targetId': target_id,
                    ':subscriptionType': 'ARTIST'
                }
            )
        elif subscriptionType == 'GENRE':
            if not target_name:
                raise ValueError("target_name must be provided for GENRE subscription")
            response = table.scan(
                FilterExpression='#username = :username AND #targetName = :targetName AND #subscriptionType = :subscriptionType',
                ExpressionAttributeNames={
                    '#username': 'username',
                    '#targetName': 'targetName',
                    '#subscriptionType': 'subscriptionType'
                },
                ExpressionAttributeValues={
                    ':username': username,
                    ':targetName': target_name,
                    ':subscriptionType': 'GENRE'
                }
            )
        else:
            raise ValueError("Invalid target_type. Must be 'ARTIST' or 'GENRE'")

        items = response.get('Items', [])
        return len(items) > 0

    except Exception as e:
        logger.error(f"Error checking subscription: {str(e)}")
        raise

def transform_rating_for_response(item):
    """Transform DynamoDB item to frontend-friendly format"""
    return {
        'ratingId': item.get('ratingId'),
        'username': item.get('username'),
        'songId': item.get('songId'),
        'stars': item.get('stars'),
        'timestamp': item.get('timestamp')
    }

def create_success_response(status_code, data):
    """Create standardized success response"""
    return {
        'statusCode': status_code,
        'headers': get_cors_headers(),
        'body': json.dumps(data, default=str)
    }

def create_error_response(status_code, message, details=None):
    """Create standardized error response"""
    error_data = {
        'error': message,
        'timestamp': datetime.utcnow().isoformat()
    }
    if details:
        error_data['details'] = details
    
    return {
        'statusCode': status_code,
        'headers': get_cors_headers(),
        'body': json.dumps(error_data)
    }

def get_cors_headers():
    """Get CORS headers for API responses"""
    return {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type,Authorization,X-Requested-With',
        'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',
        'Content-Type': 'application/json'
    }