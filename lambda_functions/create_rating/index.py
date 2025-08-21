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
    Implements requirement 1.3: Kreiranje ocene 
    """
    
    logger.info("Create artist request received")
    
    try:
        
        # Parse request body
        if not event.get('body'):
            return create_error_response(400, "Request body is required")
        
        body = json.loads(event['body'])
        
        # Create artist
        rating_id = str(uuid.uuid4())
        rating_data = create_rating_record(rating_id, body, event)
        
        # Store in DynamoDB
        store_rating(rating_data)
        
        logger.info(f"Rating created successfully: {rating_id}")
        
        return create_success_response(201, {
            'message': 'Rating created successfully',
            'artist': {
                'ratingId': rating_id,
                'songId': rating_data['songId'],
                'userId': rating_data['userId'],
                'stars': rating_data['stars'],
                'timestamp': datetime.now().isoformat()
            }
        })
        
    except Exception as e:
        logger.error(f"Create rating error: {str(e)}")
        return create_error_response(500, "Internal server error")

def create_rating_record(rating_id, input_data, event):
    """Create rating record structure"""
    
    # Get creator info from authorizer context
    request_context = event.get('requestContext', {})
    authorizer = request_context.get('authorizer', {})
    
    return {
        'ratingId': rating_id,
        'songId': input_data['songId'],
        'userId': input_data['userId'],
        'stars': input_data['stars'],
        'timestamp': datetime.now().isoformat()
    }

def store_rating(rating_data):
    """Store rating data in DynamoDB"""
    try:
        table = dynamodb.Table(os.environ['RATINGS_TABLE'])
        table.put_item(Item=rating_data)
        logger.info(f"Rating stored successfully: {rating_data['ratingId']}")
        
    except Exception as e:
        logger.error(f"Error storing rating: {str(e)}")
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