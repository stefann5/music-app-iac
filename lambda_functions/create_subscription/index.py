import json
from xml.dom.minidom import Attr
import boto3
import uuid
import os
from datetime import datetime
import logging
from boto3.dynamodb.conditions import Key

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
            'subscription': {
                'subscriptionId': subscription_id,
                'username': subscription_data['username'],
                'subscriptionType': subscription_data['subscriptionType'],
                'targetId': subscription_data['targetId'],
                'targetName': subscription_data['targetName'],
                'timestamp': subscription_data['timestamp'],
            }
        })
        
    except Exception as e:
        logger.error(f"Create Subscription error: {str(e)}")
        return create_error_response(500, "Internal server error")

def create_subscription_record(subscription_id, input_data, event):
    """Create subscription record structure"""
    
    # Get creator info from authorizer context
    request_context = event.get('requestContext', {})
    authorizer = request_context.get('authorizer', {})
    
    return {
        'subscriptionId': subscription_id,
        'username': input_data['username'],
        'subscriptionType': input_data['subscriptionType'],
        'targetId': input_data['targetId'],
        'targetName': input_data['targetName'],
        'timestamp': datetime.now().isoformat()
    }



def store_subscription(subscription_data):
    """Store subscription with duplicate check using scan (za male tabele)"""
    try:
        table = dynamodb.Table(os.environ['SUBSCRIPTIONS_TABLE'])
        
        # OPCIJA 3: Scan tabelu za postojeće subscription (jednostavno ali sporije)
        username = subscription_data['username']
        target_name = subscription_data['targetName']  
        sub_type = subscription_data.get('subscriptionType', 'artist')
        
        # Scan za duplikate
        response = table.scan(
            FilterExpression='username = :username AND targetName = :targetName AND #subscriptionType = :subscriptionType',
            ExpressionAttributeNames={'#subscriptionType': 'subscriptionType'},
            ExpressionAttributeValues={
                ':username': username,
                ':targetName': target_name,
                ':subscriptionType': sub_type
            }
        )
        
        if response['Items']:
            existing_sub = response['Items'][0]
            logger.warning(f"Duplicate subscription found: {existing_sub['subscriptionId']}")
            return create_error_response(400, 'You are already subscribed to selected content!')
        
        # Store subscription ako nema duplikata
        table.put_item(Item=subscription_data)
        logger.info(f"Subscription stored successfully: {subscription_data['subscriptionId']}")
        
    except Exception as e:
        logger.error(f"DynamoDB error: {str(e)}")
        raise
    except ValueError as e:
        # Re-raise custom validation error
        raise  
    except Exception as e:
        logger.error(f"Error storing subscription: {str(e)}")
        raise

# def store_subscription(subscription_data):
#     """Store subscription data in DynamoDB"""
#     try:
#         table = dynamodb.Table(os.environ['SUBSCRIPTIONS_TABLE'])
#         table.put_item(Item=subscription_data)
#         logger.info(f"Subscription stored successfully: {subscription_data['subscriptionId']}")
        
#     except Exception as e:
#         logger.error(f"Error storing subscription: {str(e)}")
#         raise

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