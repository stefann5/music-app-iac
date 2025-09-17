from concurrent.futures import ThreadPoolExecutor, as_completed
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
                "message": 'New content has been published by your subscription: ' + sub['targetName']
            }, event)
            notifications.append(notif)

        # 2. Sačuvaj sve u bazi
        store_notifications_batch(notifications)

        if subscriptions:
            notification = {
                'content': subscriptions[0]['targetName'],
                'message': 'New content has been published by your subscription: ' + subscriptions[0]['targetName']
            }
            send_bulk_emails(subscriptions, notification)


        return create_success_response(201, {
            "message": f"{len(notifications)} notifications created successfully",
            "notifications": notifications
        })

    except Exception as e:
        logger.error(f"Create notifications error: {str(e)}")
        return create_error_response(500, "Internal server error")

def send_bulk_emails(subscribers_list, notification):
    """Send email to all subscribers in the list"""
    
    print(f"Sending emails to {len(subscribers_list)} subscribers")
    
    # Parallel processing za brže slanje
    with ThreadPoolExecutor(max_workers=10) as executor:
        # Submit email jobs
        email_jobs = []
        for subscriber in subscribers_list:
            user_email = get_user_email(subscriber['username'])  # ili subscriber.get('email')
            if user_email:
                job = executor.submit(send_single_email, user_email, notification)
                email_jobs.append(job)
        
        # Wait for completion and log results
        success_count = 0
        for job in as_completed(email_jobs):
            try:
                job.result()  # Get result (will raise exception if failed)
                success_count += 1
            except Exception as e:
                print(f"Email sending failed: {str(e)}")
    
    print(f"Successfully sent {success_count} out of {len(email_jobs)} emails")


def send_single_email(to_email, notification):
    """Send email to single recipient"""
    
    ses = boto3.client('ses')
    
    subject = f"🎵 {notification['content']}"
    
    html_body = f"""
    <html>
    <body style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
        <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 20px; color: white; text-align: center;">
            <h1 style="margin: 0;">🎵 MusicApp</h1>
        </div>
        
        <div style="padding: 30px; background: #f9f9f9;">
            <h2 style="color: #333;">{notification['content']}</h2>
            <p style="font-size: 16px; line-height: 1.6; color: #666;">
                {notification['message']}
            </p>
        </div>
        
        <div style="padding: 20px; text-align: center; color: #999; font-size: 12px;">
            <p>You received this because you follow this artist.</p>
        </div>
    </body>
    </html>
    """
    
    text_body = f"""
    {notification['content']}
    
    {notification['message']}
    """
    
    try:
        response = ses.send_email(
            Source=os.environ['FROM_EMAIL'],
            Destination={'ToAddresses': [to_email]},
            Message={
                'Subject': {'Data': subject},
                'Body': {
                    'Html': {'Data': html_body},
                    'Text': {'Data': text_body}
                }
            }
        )
        
        print(f"Email sent to {to_email}: {response['MessageId']}")
        return True
        
    except Exception as e:
        print(f"Error sending email to {to_email}: {str(e)}")
        raise

def get_user_email(username):
    """Get user email by username using GSI - FAST!"""
    try:
        dynamodb = boto3.resource('dynamodb')
        users_table = dynamodb.Table(os.environ['USERS_TABLE'])
        
        # Query username GSI (mnogo brže od scan!)
        response = users_table.query(
            IndexName='username-index',
            KeyConditionExpression=Key('username').eq(username),
            ProjectionExpression='email'
        )
        
        items = response.get('Items', [])
        if items:
            return items[0].get('email')
        else:
            print(f"No user found with username: {username}")
            return None
        
    except Exception as e:
        print(f"Error getting user email for username {username}: {str(e)}")
        return None


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