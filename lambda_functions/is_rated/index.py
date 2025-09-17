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
        songId = query_params.get('songId')

        request_context = event.get('requestContext', {})
        authorizer = request_context.get('authorizer', {})
        username = authorizer.get('username', {})
        
        # Get subscriptions from DynamoDB
        is_rated = has_rated(songId, username)
        
        
        response_data = {
            'is_rated': is_rated,
        }
        
        return create_success_response(200, response_data)
    
    except Exception as e:
        logger.error(f"Get ratings error: {str(e)}")
        return create_error_response(500, "Internal server error")

def has_rated(song_id, username):
    """
    Check if a given user has already rated a given song.
    Returns True if exists, False otherwise.
    Uses direct GetItem instead of expensive Scan operation.
    """
    try:
        table = dynamodb.Table(os.environ['RATINGS_TABLE'])
        
        # Direct lookup using composite partition key
        rating_id = f"{song_id}#{username}"
        
        response = table.get_item(
            Key={'ratingId': rating_id}
        )
        
        # Check if item exists
        return 'Item' in response
        
    except Exception as e:
        logger.error(f"Error checking rating existence: {str(e)}")
        raise


# def has_rated(songId, username):
#     """
#     Check if a given user has already rated a given song.
#     Returns True if exists, False otherwise.
#     """
#     try:
#         table = dynamodb.Table(os.environ['RATINGS_TABLE'])

#         # Scan with FilterExpression for this user + song
#         response = table.scan(
#             FilterExpression='#username = :username AND #songId = :songId',
#             ExpressionAttributeNames={
#                 '#username': 'username',
#                 '#songId': 'songId'
#             },
#             ExpressionAttributeValues={
#                 ':username': username,
#                 ':songId': songId
#             }
#         )
#         items = response['Items']
#         return len(items) > 0

#     except Exception as e:
#         logger.error(f"Error checking rating existence: {str(e)}")
#         raise

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