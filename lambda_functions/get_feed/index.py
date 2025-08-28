import json
import boto3
import os
from typing import Dict, Any
import decimal
import logging

dynamodb = boto3.resource('dynamodb')
s3_client = boto3.client('s3')
lambda_client = boto3.client('lambda')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event, context):
    try:
        request_context = event.get('requestContext', {})
        authorizer = request_context.get('authorizer', {})
        username = authorizer.get('username', {})

        table_name = os.environ['MUSIC_CONTENT_TABLE']
        bucket_name = os.environ['MUSIC_CONTENT_BUCKET']
        table = dynamodb.Table(table_name)

        query_params = event.get('queryStringParameters', {}) or {}
        path = event.get('path', '')

        subscriptions = get_subscriptions(username)
        ratings = get_ratings(username)

        if '/stream' in path:
            return _handle_stream_request(query_params, table, bucket_name)
        
        content_id = query_params.get('contentId')
        artist_id = query_params.get('artistId')
        search_query = query_params.get('search')

        if content_id:
            return _get_content_by_id(table, content_id, bucket_name)
        elif artist_id:
            return _get_content_by_artist(artist_id, table, query_params)
        elif search_query:
            return _search_content_by_title(search_query, table, query_params)
        else:
            return _get_all_content(table, query_params)
    except Exception as e:
        print(f"Error processing request: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

def _get_content_by_id(table, content_id, bucket_name):
    try:
        response = table.get_item(Key={'contentId': content_id})

        if 'Item' not in response:
            return {
                'statusCode': 404,
                'body': json.dumps({'error': 'Content not found'})
            }
        item = response['Item']

        stream_url = _generate_stream_url(item, bucket_name)
        safe_item = _sanitize_item(item)
        safe_item['streamUrl'] = stream_url

        return {
            'statusCode': 200,
            'body': json.dumps({'content': safe_item})
        }
    except Exception as e:
        print(f"Error fetching content by ID: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Failed to get content by ID'})
        }
    
def _generate_stream_url(item: Dict[str, Any], bucket_name: str, expires_in: int = 3600):
    try:
        persigned_url = s3_client.generate_presigned_url(
            'get_object',
            Params={
                'Bucket': bucket_name, 
                'Key': item['s3Key'],
                'ResponseContentType': item.get('fileType', 'audio/mpeg'),
                'ResponseContentDisposition': f'inline; filename="{item["filename"]}"'
            },
            ExpiresIn=expires_in
        )
        return persigned_url
    except Exception as e:
        print(f"Error generating presigned URL: {str(e)}")
        return ""

#Removes sensitive fields from DynamoDB item before returning to client
def _sanitize_item(item):
    safe_item = dict(item)
    #Remove sensitive fields
    safe_fields_to_remove = ['bucketName', 's3Key', 'coverImageS3Key']
    for field in safe_fields_to_remove:
        safe_item.pop(field, None)
    #Convert Decimal types to float for JSON serialization
    for key, value in safe_item.items():
        if isinstance(value, decimal.Decimal):
            safe_item[key] = float(value)
        else:
            safe_item[key] = value
    
    return safe_item

def _get_content_by_artist(artist_id, table, query_params):
    try:
        limit = min(int(query_params.get('limit', 50)), 100)
        last_key = query_params.get('lastKey')

        query_kwargs = {
            'IndexName': 'artistId-index',
            'KeyConditionExpression': 'artistId = :artistId',
            'ExpressionAttributeValues': {':artistId': artist_id},
            'Limit': limit
        }

        if last_key:
            query_kwargs['ExclusiveStartKey'] = { 'contentId': last_key }

        response = table.query(**query_kwargs)
        items = [_sanitize_item(item) for item in response.get('Items', [])]

        result = {
            'content': items,
            'count': len(items),
            'artistId': artist_id
        }

        if 'LastEvaluatedKey' in response:
            result['lastKey'] = response['LastEvaluatedKey']['contentId']

        return {
            'statusCode': 200,
            'body': json.dumps(result)
        }
    except Exception as e:
        print(f"Error fetching content by artist: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Failed to get content by artist'})
        }

def _search_content_by_title(search_query, table, query_params):
    try:
        limit = min(int(query_params.get('limit', 50)), 100)

        scan_kwargs = {
            'FilterExpression': 'contains(#title, :search)',
            'ExpressionAttributeNames': {'#title': 'title'},
            'ExpressionAttributeValues': [':search', search_query],
            'Limit': limit
        }

        response = table.scan(**scan_kwargs)
        items = [_sanitize_item(item) for item in response.get('Items', [])]

        result = {
            'content': items,
            'count': len(items),
            'searchQuery': search_query
        }

        if 'LastEvaluatedKey' in response:
            result['lastKey'] = response['LastEvaluatedKey']['contentId']

        return {
            'statusCode': 200,
            'body': json.dumps(result)
        }
    except Exception as e:
        print(f"Error searching content by title: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Failed to search content by title'})
        }

def _get_all_content(table, query_params):
    try:
        limit = min(int(query_params.get('limit', 50)), 100)
        last_key = query_params.get('lastKey')

        scan_kwargs = {
            'Limit': limit
        }

        if last_key:
            scan_kwargs['ExclusiveStartKey'] = { 'contentId': last_key }

        response = table.scan(**scan_kwargs)
        items = [_sanitize_item(item) for item in response.get('Items', [])]

        result = {
            'content': items,
            'count': len(items)
        }

        if 'LastEvaluatedKey' in response:
            result['lastKey'] = response['LastEvaluatedKey']['contentId']

        return {
            'statusCode': 200,
            'body': json.dumps(result)
        }
    except Exception as e:
        print(f"Error fetching all content: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Failed to get all content'})
        }

def _handle_stream_request(query_params: Dict[str, Any], table, bucket_name: str):
    content_id = query_params.get('contentId')
    if not content_id:
        return {
            'statusCode': 400,
            'body': json.dumps({'error': 'contentId is required for streaming'})
        }
    
    try:
        response = table.get_item(Key={'contentId': content_id})

        if 'Item' not in response:
            return {
                'statusCode': 404,
                'body': json.dumps({'error': 'Content not found'})
            }
        
        item = response['Item']
        presigned_url = _generate_stream_url(item, bucket_name)
        item = _sanitize_item(item)
        return {
            'statusCode': 200,
            'body': json.dumps({
                'streamUrl': presigned_url,
                'contentId': content_id,
                'title': item['title'],
                'filename': item['filename'],
                'fileType': item.get('fileType', 'audio/mpeg'),
                'fileSize': item.get('fileSize'),
                'duration': item.get('duration'),
                'artistId': item.get('artistId'),
                'album': item.get('album'),
                'genre': item.get('genre'),
                'expiresIn': 3600
                })
        }
    except Exception as e:
        print(f"Error generating presigned URL: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Failed to generate stream URL'})
        }

def get_subscriptions(username):
    """Get subscriptions from DynamoDB with optional pagination and filtering"""
    try:
        # Proveri da li postoji environment varijabla
        table_name = os.environ.get('SUBSCRIPTIONS_TABLE')
        if not table_name:
            logger.error("SUBSCRIPTIONS_TABLE environment variable is not set")
            raise ValueError("SUBSCRIPTIONS_TABLE environment variable is not set")
        
        table = dynamodb.Table(table_name)
        
        # Scan parameters - ispravio sam i FilterExpression
        scan_params = {
            'FilterExpression': '#username = :username',  # Tačno poklapanje umesto contains
            'ExpressionAttributeNames': {
                '#username': 'username'
            },
            'ExpressionAttributeValues': {
                ':username': username
            }
        }
        
        # Perform scan
        response = table.scan(**scan_params)
        
        # Transform subscriptions data for frontend
        subscriptions = []
        for item in response.get('Items', []):
            subscription = transform_subscription_for_response(item)
            subscriptions.append(subscription)
        
        result = subscriptions  # Ispravio sam - treba da bude subscriptions, ne subscription
        
        return result
        
    except Exception as e:
        logger.error(f"Error getting subscriptions: {str(e)}")
        raise

# def get_subscriptions(username):
#     """Get subscriptions from DynamoDB with optional pagination and filtering"""
#     try:
#         table = dynamodb.Table(os.environ['SUBSCRIPTIONS_TABLE'])
        
#             # Scan parameters
#         scan_params = {
            
#         }

#         scan_params['FilterExpression'] = 'contains(username, :username)'
#         scan_params['ExpressionAttributeValues'] = {':username': username}
        
        
#         # Perform scan
#         response = table.scan(**scan_params)
        
#         # Transform artists data for frontend
#         subscriptions = []
#         for item in response.get('Items', []):
#             subscription = transform_subscription_for_response(item)
#             subscriptions.append(subscription)
        
#         result = subscriptions
        
#         return result
        
#     except Exception as e:
#         logger.error(f"Error getting subscriptions: {str(e)}")
#         raise

def transform_subscription_for_response(item):
    """Transform DynamoDB item to frontend-friendly format"""
    return {
        'subscriptionId': item.get('subscriptionId'),
        'username': item.get('username'), 
        'targetId': item.get('targetId'),
        'targetName': item.get('targetName'),
        'timestamp': item.get('timestamp')
    }

def get_ratings(username):
    """Get ratings from DynamoDB with optional pagination and filtering"""
    try:
        table = dynamodb.Table(os.environ['RATINGS_TABLE'])
        
        # Scan parameters
        scan_params = {
        }

        scan_params['FilterExpression'] = 'contains(username, :username)'
        scan_params['ExpressionAttributeValues'] = {':username': username}
        
        # Perform scan
        response = table.scan(**scan_params)
        
        # Transform artists data for frontend
        ratings = []
        for item in response.get('Items', []):
            rating = transform_rating_for_response(item)
            ratings.append(rating)
        
        result = ratings
        
        return result
        
    except Exception as e:
        logger.error(f"Error getting ratings: {str(e)}")
        raise

def transform_rating_for_response(item):
    """Transform DynamoDB item to frontend-friendly format"""
    return {
        'ratingId': item.get('ratingId'),
        'username': item.get('username'),
        'songId': item.get('songId'),
        'stars': item.get('stars'),
        'timestamp': item.get('timestamp')
    }