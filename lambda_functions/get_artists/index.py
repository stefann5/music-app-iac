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
    Get All Artists Handler
    Protected endpoint for retrieving all artists
    """
    
    logger.info("Get all artists request received")
    
    try:
        # Parse query parameters
        query_params = event.get('queryStringParameters') or {}
        limit = int(query_params.get('limit', 50))  # Default limit of 50
        last_key = query_params.get('lastKey')  # For pagination
        genre_filter = query_params.get('genre')  # Optional genre filter
        
        # Validate limit
        if limit > 100:
            limit = 100  # Maximum limit of 100
        
        # Get artists from DynamoDB
        artists_data = get_artists(limit, last_key, genre_filter)
        
        logger.info(f"Retrieved {len(artists_data['artists'])} artists")
        
        response_data = {
            'message': 'Artists retrieved successfully',
            'artists': artists_data['artists'],
            'count': len(artists_data['artists']),
            'hasMore': artists_data.get('hasMore', False)
        }
        
        if artists_data.get('lastKey'):
            response_data['lastKey'] = artists_data['lastKey']
        
        return create_success_response(200, response_data)
        
    except Exception as e:
        logger.error(f"Get artists error: {str(e)}")
        return create_error_response(500, "Internal server error")

def get_artists(limit, last_key=None, genre_filter=None):
    """Get artists from DynamoDB with optional pagination and filtering"""
    try:
        table = dynamodb.Table(os.environ['ARTISTS_TABLE'])
        
        # Scan parameters
        scan_params = {
            'Limit': limit,
            'FilterExpression': '#status = :status',
            'ExpressionAttributeNames': {'#status': 'status'},
            'ExpressionAttributeValues': {':status': 'active'}
        }
        
        # Add genre filter if specified
        if genre_filter:
            scan_params['FilterExpression'] = '#status = :status AND contains(genres, :genre)'
            scan_params['ExpressionAttributeValues'][':genre'] = genre_filter.lower()
        
        # Add pagination if last key is provided
        if last_key:
            try:
                # Decode the last key from base64 if needed
                import base64
                decoded_key = json.loads(base64.b64decode(last_key).decode('utf-8'))
                scan_params['ExclusiveStartKey'] = decoded_key
            except Exception as e:
                logger.warning(f"Invalid lastKey format: {str(e)}")
        
        # Perform scan
        response = table.scan(**scan_params)
        
        # Transform artists data for frontend
        artists = []
        for item in response.get('Items', []):
            artist = transform_artist_for_response(item)
            artists.append(artist)
        
        # Sort by name for consistent ordering
        artists.sort(key=lambda x: x['name'].lower())
        
        result = {
            'artists': artists,
            'hasMore': 'LastEvaluatedKey' in response
        }
        
        # Include last key for pagination
        if 'LastEvaluatedKey' in response:
            import base64
            last_key_encoded = base64.b64encode(
                json.dumps(response['LastEvaluatedKey'], default=str).encode('utf-8')
            ).decode('utf-8')
            result['lastKey'] = last_key_encoded
        
        return result
        
    except Exception as e:
        logger.error(f"Error getting artists: {str(e)}")
        raise

def transform_artist_for_response(item):
    """Transform DynamoDB item to frontend-friendly format"""
    return {
        'artistId': item.get('artistId'),
        'name': item.get('name'),
        'biography': item.get('biography'),
        'genres': item.get('genres', []),
        'country': item.get('country', ''),
        'formedYear': item.get('formedYear'),
        'members': item.get('members', []),
        'imageUrl': item.get('imageUrl', ''),
        'socialLinks': item.get('socialLinks', {}),
        'metadata': {
            'totalSongs': item.get('metadata', {}).get('totalSongs', 0),
            'totalAlbums': item.get('metadata', {}).get('totalAlbums', 0),
            'followers': item.get('metadata', {}).get('followers', 0),
            'verified': item.get('metadata', {}).get('verified', False)
        },
        'createdAt': item.get('createdAt'),
        'updatedAt': item.get('updatedAt')
    }

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