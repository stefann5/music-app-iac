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
    Protected endpoint for retrieving all subscriptions
    """
    
    logger.info("Get all subscriptions request received")
    
    try:
        # Parse query parameters
        query_params = event.get('queryStringParameters') or {}
        limit = int(query_params.get('limit', 50))  # Default limit of 50
        last_key = query_params.get('lastKey')  # For pagination

        request_context = event.get('requestContext', {})
        authorizer = request_context.get('authorizer', {})
        username = authorizer.get('username', {})

        targetName = query_params.get('targetName')

        if targetName:
            username = None

        # Validate limit
        if limit > 100:
            limit = 100  # Maximum limit of 100
        
        subscriptions_data = get_subscriptions(limit, last_key, username, targetName)
        
        
        logger.info(f"Retrieved {len(subscriptions_data['subscriptions'])} subscriptions")
        
        response_data = {
            'message': 'Subscriptions retrieved successfully',
            'subscriptions': subscriptions_data['subscriptions'],
            'count': len(subscriptions_data['subscriptions']),
            'hasMore': subscriptions_data.get('hasMore', False)
        }
        
        if subscriptions_data.get('lastKey'):
            response_data['lastKey'] = subscriptions_data['lastKey']
        
        return create_success_response(200, response_data)
        
    except Exception as e:
        logger.error(f"Get subscriptions error: {str(e)}")
        return create_error_response(500, "Internal server error")

def get_subscriptions(limit, last_key=None, username=None, targetName=None):
    """Get subscriptions from DynamoDB with optional pagination and filtering"""
    try:
        table = dynamodb.Table(os.environ['SUBSCRIPTIONS_TABLE'])
        
            # Scan parameters
        scan_params = {
            'Limit': limit
        }

        # Add username or targetName filter if specified
        if username:
            scan_params['FilterExpression'] = 'contains(username, :username)'
            scan_params['ExpressionAttributeValues'] = {':username': username}

        if targetName:
            scan_params['FilterExpression'] = 'contains(targetName, :targetName)'
            scan_params['ExpressionAttributeValues'] = {':targetName': targetName}
        
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
        subscriptions = []
        for item in response.get('Items', []):
            subscription = transform_subscription_for_response(item)
            subscriptions.append(subscription)
        
        # Sort by name for consistent ordering
        subscriptions.sort(key=lambda x: x['targetName'].lower())
        
        result = {
            'subscriptions': subscriptions,
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
        logger.error(f"Error getting subscriptions: {str(e)}")
        raise

def transform_subscription_for_response(item):
    """Transform DynamoDB item to frontend-friendly format"""
    return {
        'subscriptionId': item.get('subscriptionId'),
        'username': item.get('username'), 
        'targetId': item.get('targetId'),
        'targetName': item.get('targetName'),
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