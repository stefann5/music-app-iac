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
    Get All Artists Handler
    Protected endpoint for retrieving all notifications
    """
    
    logger.info("Get all notifications request received")
    
    try:
        # Parse query parameters
        query_params = event.get('queryStringParameters') or {}
        limit = int(query_params.get('limit', 50))  # Default limit of 50
        last_key = query_params.get('lastKey')  # For pagination
        
        request_context = event.get('requestContext', {})
        authorizer = request_context.get('authorizer', {})
        subscriber = authorizer.get('username', {})

        # Validate limit
        if limit > 100:
            limit = 100  # Maximum limit of 100
        
        notifications_data = get_notifications(limit, last_key, subscriber)
        
        
        logger.info(f"Retrieved {len(notifications_data['notifications'])} notifications")
        
        response_data = {
            'message': 'Notifications retrieved successfully',
            'notifications': notifications_data['notifications'],
            'count': len(notifications_data['notifications']),
            'hasMore': notifications_data.get('hasMore', False)
        }
        
        if notifications_data.get('lastKey'):
            response_data['lastKey'] = notifications_data['lastKey']
        
        return create_success_response(200, response_data)
        
    except Exception as e:
        logger.error(f"Get notifications error: {str(e)}")
        return create_error_response(500, "Internal server error")

def get_notifications(limit, last_key=None, subscriber=None):
    """Get notifications from DynamoDB with optional pagination and filtering"""
    try:
        table = dynamodb.Table(os.environ['NOTIFICATIONS_TABLE'])
        
            # Scan parameters
        scan_params = {
            'Limit': limit
        }

        # Add username or targetName filter if specified
        if subscriber:
            scan_params['FilterExpression'] = 'contains(subscriber, :subscriber)'
            scan_params['ExpressionAttributeValues'] = {':subscriber': subscriber}
        
        # Add pagination if last key is provided
        if last_key:
            try:
                # Decode the last key from base64 if needed
                import base64
                decoded_key = json.loads(base64.b64decode(last_key).decode('utf-8'))
                scan_params['ExclusiveStartKey'] = decoded_key
            except Exception as e:
                logger.warning(f"Invalid lastKey format: {str(e)}")
        
        # Perform scan
        response = table.scan(**scan_params)
        
        # Transform artists data for frontend
        notifications = []
        for item in response.get('Items', []):
            notification = transform_notification_for_response(item)
            notifications.append(notification)
        
        # Sort by name for consistent ordering
        notifications.sort(key=lambda x: x['timestamp'].lower())
        
        result = {
            'notifications': notifications,
            'hasMore': 'LastEvaluatedKey' in response
        }
        
        # Include last key for pagination
        if 'LastEvaluatedKey' in response:
            import base64
            last_key_encoded = base64.b64encode(
                json.dumps(response['LastEvaluatedKey'], default=str).encode('utf-8')
            ).decode('utf-8')
            result['lastKey'] = last_key_encoded
        
        return result
        
    except Exception as e:
        logger.error(f"Error getting notifications: {str(e)}")
        raise

def transform_notification_for_response(item):
    """Transform DynamoDB item to frontend-friendly format"""
    return {
        'notificationId': item.get('notificationId'),
        'subscriber': item.get('subscriber'), 
        'contentId': item.get('contentId'),
        'content': item.get('content'),
        'message': item.get('message'),
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