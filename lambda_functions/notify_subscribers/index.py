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
    Create Notifications Handler
    Implements requirement 1.3: Kreiranje notifikacija za više subscribera
    """
    logger.info("Create notifications request received")

    try:
        # Parse request body
        if not event.get('body'):
            return create_error_response(400, "Request body is required")

        body = json.loads(event['body'])

        # Proveri da li ima subscriptions liste
        subscriptions = body.get('subscriptions')
        if not subscriptions or not isinstance(subscriptions, list):
            return create_error_response(400, "Field 'subscriptions' must be a non-empty list")

        # 1. Kreiraj notifikacije za sve subscribere
        notifications = []
        for sub in subscriptions:
            notification_id = str(uuid.uuid4())

            if not sub['targetId']:
                sub['targetId'] = '' 

            notif = create_notification_record(notification_id, {
                "subscriber": sub['username'],
                "contentId": sub['targetId'],
                "content": sub['targetName'],
                "message": 'New content has been published by your subscription ' + sub['targetName']
            }, event)
            notifications.append(notif)

        # 2. Sačuvaj sve u bazi
        store_notifications_batch(notifications)

        return create_success_response(201, {
            "message": f"{len(notifications)} notifications created successfully",
            "notifications": notifications
        })

    except Exception as e:
        logger.error(f"Create notifications error: {str(e)}")
        return create_error_response(500, "Internal server error")

def create_notification_record(notification_id, input_data, event):
    """Create Notification record structure"""
    
    # Get creator info from authorizer context
    request_context = event.get('requestContext', {})
    authorizer = request_context.get('authorizer', {})
    
    return {
        'notificationId': notification_id,
        'subscriber': input_data['subscriber'],
        'contentId': input_data['contentId'],
        'content': input_data['content'],
        'message': input_data['message'],
        'timestamp': datetime.now().isoformat()
    }

def store_notification(notification_data):
    """Store notification data in DynamoDB"""
    try:
        table = dynamodb.Table(os.environ['NOTIFICATIONS_TABLE'])
        table.put_item(Item=notification_data)
        logger.info(f"Notification stored successfully: {notification_data['notificationId']}")
        
    except Exception as e:
        logger.error(f"Error storing notification: {str(e)}")
        raise

def store_notifications_batch(notifications):
    """Store multiple notifications in DynamoDB using batch_writer"""
    try:
        table = dynamodb.Table(os.environ["NOTIFICATIONS_TABLE"])
        with table.batch_writer() as batch:
            for notif in notifications:
                batch.put_item(Item=notif)
        logger.info(f"{len(notifications)} notifications stored successfully")

    except Exception as e:
        logger.error(f"Error storing notifications: {str(e)}")
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