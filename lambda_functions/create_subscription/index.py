import json
import boto3
import uuid
import os
from datetime import datetime
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')

def handler(event, context):
    """
    Create Artist Handler
    Implements requirement 1.3: Kreiranje pretplate 
    """
    
    logger.info("Create subscription request received")
    
    try:
        
        # Parse request body
        if not event.get('body'):
            return create_error_response(400, "Request body is required")
        
        body = json.loads(event['body'])
        
        # Create artist
        subscription_id = str(uuid.uuid4())
        subscription_data = create_subscription_record(subscription_id, body, event)
        
        # Store in DynamoDB
        store_subscription(subscription_data)
        
        logger.info(f"Subscription created successfully: {subscription_id}")
        
        return create_success_response(201, {
            'message': 'Subscription created successfully',
            'artist': {
                'subscriptionId': subscription_id,
                'userId': subscription_data['userId'],
                'subscriptionType': subscription_data['subscriptionType'],
                'targetId': subscription_data['targetId'],
                'timestamp': subscription_data['timestamp'],
            }
        })
        
    except Exception as e:
        logger.error(f"Create Subscription error: {str(e)}")
        return create_error_response(500, "Internal server error")

def create_subscription_record(subscription_id, subscription_data, event):
    """Create subscription record structure"""
    
    # Get creator info from authorizer context
    request_context = event.get('requestContext', {})
    authorizer = request_context.get('authorizer', {})
    
    return {
        'subscriptionId': subscription_id,
        'userId': subscription_data['userId'],
        'subscriptionType': subscription_data['subscriptionType'],
        'targetId': subscription_data['targetId'],
        'timestamp': datetime.now().isoformat()
    }

def store_subscription(subscription_data):
    """Store subscription data in DynamoDB"""
    try:
        table = dynamodb.Table(os.environ['SUBSCRIPTIONS_TABLE'])
        table.put_item(Item=subscription_data)
        logger.info(f"Subscription stored successfully: {subscription_data['subscription_id']}")
        
    except Exception as e:
        logger.error(f"Error storing subscription: {str(e)}")
        raise

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