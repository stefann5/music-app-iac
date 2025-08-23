import json
import boto3
import os
from datetime import datetime
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')

def handler(event, context):
    """
    Delete Subscription Handler
    Deletes a subscription by subscriptionId
    """
    
    logger.info("Delete subscription request received")
    
    try:
        # Parse path parameters (API Gateway path like /subscription/{subscriptionId})
        path_params = event.get('pathParameters') or {}
        subscription_id = path_params.get('subscriptionId')
        
        if not subscription_id:
            return create_error_response(400, "subscriptionId is required in path parameters")
        
        # Delete from DynamoDB
        deleted_item = delete_subscription(subscription_id)
        
        if not deleted_item:
            return create_error_response(404, f"Subscription {subscription_id} not found")
        
        logger.info(f"Subscription deleted successfully: {subscription_id}")
        
        return create_success_response(200, {
            'message': 'Subscription deleted successfully',
            'subscriptionId': subscription_id
        })
        
    except Exception as e:
        logger.error(f"Delete Subscription error: {str(e)}")
        return create_error_response(500, "Internal server error")

def delete_subscription(subscription_id):
    """Delete subscription from DynamoDB by subscriptionId"""
    try:
        table = dynamodb.Table(os.environ['SUBSCRIPTIONS_TABLE'])
        
        # Delete item and return old values
        response = table.delete_item(
            Key={'subscriptionId': subscription_id},
            ReturnValues='ALL_OLD'  # vraća obrisani item, None ako ne postoji
        )
        
        return response.get('Attributes')
        
    except Exception as e:
        logger.error(f"Error deleting subscription: {str(e)}")
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
