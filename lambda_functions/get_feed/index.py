from collections import defaultdict
from datetime import datetime, timedelta
import json
import boto3
import os
from typing import Counter, Dict, Any
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
        
        feed_albums = get_feed_albums(username)

        feed_albums = convert_decimals_to_float(feed_albums)

        return {
            'statusCode': 200,
            'headers': get_cors_headers(),
            'body': json.dumps(feed_albums)
        }

        
    except Exception as e:
        print(f"Error processing request: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

def get_feed_albums(username):
    try:
        dynamodb = boto3.resource("dynamodb")
        table = dynamodb.Table(os.environ["FEED_TABLE"])    

        response = table.get_item(
            Key={"username": username},
            ProjectionExpression="feed" 
        )
        
        if "Item" not in response:
            return []
        
        # feed je lista albuma
        return response["Item"]["feed"]

    except Exception as e:
        print(f"Error fetching feed for {username}: {str(e)}")
        raise

def convert_decimals_to_float(obj):
    """Rekurzivno konvertuje sve Decimal objekte u float"""
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    elif isinstance(obj, dict):
        return {key: convert_decimals_to_float(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_decimals_to_float(item) for item in obj]
    else:
        return obj
    
def convert_floats_to_decimal(obj):
    """Rekurzivno konvertuje sve Decimal objekte u float"""
    if isinstance(obj, float):
        return decimal.Decimal(obj)
    elif isinstance(obj, dict):
        return {key: convert_floats_to_decimal(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_floats_to_decimal(item) for item in obj]
    else:
        return obj


def transform_album_for_response(item):
    """Transform DynamoDB album item to frontend-friendly format"""
    # Convert Decimal to float for JSON serialization
    for key, value in item.items():
        if isinstance(value, decimal.Decimal):
            item[key] = float(value)
    
    return {
        'albumId': item.get('albumId'),
        'title': item.get('title'),
        'artistId': item.get('artistId'),
        'genre': item.get('genre'),
        'description': item.get('description', ''),
        'releaseYear': item.get('releaseYear'),
        'trackCount': item.get('trackCount', 0),
        'duration': item.get('duration', 0),
        'coverImageUrl': item.get('coverImageUrl', ''),
        'status': item.get('status'),
        'createdAt': item.get('createdAt'),
        'updatedAt': item.get('updatedAt'),
        'metadata': item.get('metadata', {}),
        'recordLabel': item.get('recordLabel', ''),
        'producer': item.get('producer', ''),
        'tags': item.get('tags', []),
        'isExplicit': item.get('isExplicit', False),
        'stats': {
            'score': 0
        }   
    }

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

def get_cors_headers():
    """Get CORS headers for API responses"""
    return {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type,Authorization,X-Requested-With',
        'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',
        'Content-Type': 'application/json'
    }