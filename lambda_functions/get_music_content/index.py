import json
import boto3
import os
from typing import Dict, Any
import decimal

dynamodb = boto3.resource('dynamodb')
s3_client = boto3.client('s3')

def handler(event, context):
    try:
        table_name = os.environ['MUSIC_CONTENT_TABLE']
        bucket_name = os.environ['MUSIC_CONTENT_BUCKET']
        table = dynamodb.Table(table_name)

        query_params = event.get('queryStringParameters', {}) or {}
        path = event.get('path', '')

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