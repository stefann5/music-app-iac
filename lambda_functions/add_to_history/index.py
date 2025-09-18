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
        
        if is_admin_user(event):
            return {
                'statusCode': 201,
                'headers': get_cors_headers(),
                'body': json.dumps({'message': ''})
            }

        # Parse request body
        if not event.get('body'):
            return create_error_response(400, "Request body is required")
        
        body = json.loads(event['body'])

        request_context = event.get('requestContext', {})
        authorizer = request_context.get('authorizer', {})
        username = authorizer.get('username', {})

        query_params = event.get('queryStringParameters') or {}
        contentId = query_params.get('contentId')

        # Store in DynamoDB
        add_to_history(contentId, username)
        
        trigger_feed_calculation(
            username=username,
        )


        return create_success_response(201, {
            'message': 'History edited successfully'
        })
        
    except Exception as e:
        logger.error(f"Create rating error: {str(e)}")
        return create_error_response(500, "Internal server error")



def create_rating_record(rating_id, input_data, event):
    """Create rating record structure"""
    
    # Get creator info from authorizer context
    request_context = event.get('requestContext', {})
    authorizer = request_context.get('authorizer', {})
    username = authorizer.get('username', {})
    
    return {
        'ratingId': rating_id,
        'songId': input_data['songId'],
        'username': username,
        'stars': input_data['stars'],
        'timestamp': datetime.now().isoformat()
    }


def is_admin_user(event):
    """Check if the user has administrator role"""
    try:
        request_context = event.get('requestContext', {})
        authorizer = request_context.get('authorizer', {})
        
        # Check if user is in administrators group
        groups = authorizer.get('groups', '').split(',')
        role = authorizer.get('role', '')
        
        return 'administrators' in groups or role == 'admin'
        
    except Exception as e:
        print(f"Error checking admin role: {str(e)}")
        return False

def add_to_history(contentId, username):
    """Store rating with duplicate check using scan (for small tables)"""

    mus_table_name = os.environ.get('MUSIC_CONTENT_TABLE')
    if not mus_table_name:
        raise ValueError("MUSIC_CONTENT_TABLE environment variable is not set")
        
    mus_table = dynamodb.Table(mus_table_name)
    response = mus_table.get_item(Key={'contentId': contentId})

    if 'Item' not in response:
        return {
                'statusCode': 404,
                'headers': get_cors_headers(),
                'body': json.dumps({'error': 'Content not found'})
            }
        
    item = response['Item']

    art_table_name = os.environ.get('ARTISTS_TABLE')
    if not art_table_name:
        raise ValueError("ARTISTS_TABLE environment variable is not set")
        
    art_table = dynamodb.Table(art_table_name)
    artist = art_table.get_item(Key={'artistId': item['artistId']})

    user_table_name = os.environ.get('USERS_TABLE')
    if not user_table_name:
        raise ValueError("USERS_TABLE environment variable is not set")
        
    user_table = dynamodb.Table(user_table_name)

    # Scan with FilterExpression for this user + song
    response = user_table.scan(
            FilterExpression='#username = :username',
            ExpressionAttributeNames={
                '#username': 'username'
            },
            ExpressionAttributeValues={
                ':username': username
            }
        )
    items = response['Items']
        

    print(items)
        # Korak 2: Uzmi userId (partition key)
    user_id = items[0]['userId']
        
    history_item = {
            'genre': item['genre'],
            'artist': item['artistId'],
            'timestamp': datetime.now().isoformat()
    }

    user_table.update_item(
            Key={'userId': user_id},
            UpdateExpression='SET stats.llisteningHistory = list_append(if_not_exists(stats.llisteningHistory, :empty_list), :new_item)',
            ExpressionAttributeValues={
                ':new_item': [history_item],
                ':empty_list': []
            }
    )


# def store_rating(rating_data):
#     """Store rating data in DynamoDB"""
#     try:
#         table = dynamodb.Table(os.environ['RATINGS_TABLE'])
#         table.put_item(Item=rating_data)
#         logger.info(f"Rating stored successfully: {rating_data['ratingId']}")
        
#     except Exception as e:
#         logger.error(f"Error storing rating: {str(e)}")
#         raise

def create_success_response(status_code, data):
    """Create standardized success response"""
    return {
        'statusCode': status_code,
        'headers': get_cors_headers(),
        'body': json.dumps(data, default=str)
    }

def trigger_feed_calculation(username):
    """Trigger feed calculation after history update"""
    
    lambda_client = boto3.client('lambda')
    
    payload = {
        'username': username,
        'action': 'history_updated',
        'timestamp': datetime.now().isoformat()
    }
    
    # Invoke calculate feed function asynchronously
    lambda_client.invoke(
        FunctionName=os.environ['CALCULATE_FEED_FUNCTION'],
        InvocationType='Event',  # Async invocation
        Payload=json.dumps(payload)
    )
    
    print(f"Feed calculation triggered for user: {username}")

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