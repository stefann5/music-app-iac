import json
import boto3
import os
from datetime import datetime
import logging
import base64
from boto3.dynamodb.conditions import Key
from decimal import Decimal

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')

def handler(event, context):
    """
    Get Albums Handler - Performance optimized for discover functionality
    
    Supports:
    - GET /albums - Get all albums with pagination
    - GET /albums?artistId=123 - Get albums by artist
    - GET /albums?genre=rock - Get albums by genre
    - GET /albums?albumId=123 - Get specific album
    """
    
    logger.info("Get albums request received")
    
    try:
        # Parse query parameters
        query_params = event.get('queryStringParameters') or {}
        
        # Route to appropriate handler based on parameters
        album_id = query_params.get('albumId')
        artist_id = query_params.get('artistId') 
        genre = query_params.get('genre')
        
        if album_id:
            return get_album_by_id(album_id)
        elif artist_id:
            return get_albums_by_artist(query_params)
        elif genre:
            return get_albums_by_genre(query_params)
        else:
            return get_all_albums(query_params)
            
    except Exception as e:
        logger.error(f"Get albums error: {str(e)}")
        return create_error_response(500, "Internal server error")

def get_album_by_id(album_id):
    """Get specific album by ID"""
    try:
        table = dynamodb.Table(os.environ['ALBUMS_TABLE'])
        
        response = table.get_item(Key={'albumId': album_id})
        
        if 'Item' not in response:
            return create_error_response(404, "Album not found")
        
        album = transform_album_for_response(response['Item'])
        
        # Get album tracks
        tracks = get_album_tracks(album_id)
        album['tracks'] = tracks
        album['actualTrackCount'] = len(tracks)
        
        logger.info(f"Retrieved album: {album_id}")
        
        return create_success_response(200, {
            'message': 'Album retrieved successfully',
            'album': album
        })
        
    except Exception as e:
        logger.error(f"Error getting album by ID: {str(e)}")
        raise

def get_albums_by_artist(query_params):
    """
    Get albums by artist using performance-optimized GSI
    PERFORMANCE: Uses artistId-createdAt-index for fast queries
    """
    try:
        artist_id = query_params.get('artistId')
        limit = min(int(query_params.get('limit', 20)), 100)
        last_key = query_params.get('lastKey')
        sort_by = query_params.get('sortBy', 'newest')  # newest, oldest
        
        table = dynamodb.Table(os.environ['ALBUMS_TABLE'])
        
        # PERFORMANCE: Use artistId-createdAt-index for optimal query
        query_params_db = {
            'IndexName': 'artistId-createdAt-index',
            'KeyConditionExpression': Key('artistId').eq(artist_id),
            'Limit': limit,
            'ScanIndexForward': sort_by != 'newest'  # False for newest first
        }
        
        if last_key:
            query_params_db['ExclusiveStartKey'] = decode_last_key(last_key)
        
        response = table.query(**query_params_db)
        
        albums = [transform_album_for_response(item) for item in response.get('Items', [])]
        
        logger.info(f"Retrieved {len(albums)} albums for artist: {artist_id}")
        
        return create_success_response(200, {
            'message': f'Albums retrieved for artist',
            'albums': albums,
            'count': len(albums),
            'filters': {'artistId': artist_id, 'sortBy': sort_by},
            'hasMore': 'LastEvaluatedKey' in response,
            'lastKey': encode_last_key(response.get('LastEvaluatedKey')) if 'LastEvaluatedKey' in response else None
        })
        
    except Exception as e:
        logger.error(f"Error getting albums by artist: {str(e)}")
        raise

def get_albums_by_genre(query_params):
    """
    Get albums by genre using performance-optimized GSI
    PERFORMANCE: Uses genre-createdAt-index for fast queries
    """
    try:
        genre = query_params.get('genre', '').lower().strip()
        limit = min(int(query_params.get('limit', 20)), 100)
        last_key = query_params.get('lastKey')
        sort_by = query_params.get('sortBy', 'newest')  # newest, oldest
        
        if not genre:
            return create_error_response(400, "Genre parameter is required")
        
        table = dynamodb.Table(os.environ['ALBUMS_TABLE'])
        
        # PERFORMANCE: Use genre-createdAt-index for chronological albums
        query_params_db = {
            'IndexName': 'genre-createdAt-index',
            'KeyConditionExpression': Key('genre').eq(genre),
            'Limit': limit,
            'ScanIndexForward': sort_by != 'newest'  # False for newest first
        }
        
        if last_key:
            query_params_db['ExclusiveStartKey'] = decode_last_key(last_key)
        
        response = table.query(**query_params_db)
        
        albums = [transform_album_for_response(item) for item in response.get('Items', [])]
        
        logger.info(f"Retrieved {len(albums)} albums for genre: {genre}")
        
        return create_success_response(200, {
            'message': f'Albums retrieved for genre "{genre}"',
            'albums': albums,
            'count': len(albums),
            'filters': {'genre': genre, 'sortBy': sort_by},
            'hasMore': 'LastEvaluatedKey' in response,
            'lastKey': encode_last_key(response.get('LastEvaluatedKey')) if 'LastEvaluatedKey' in response else None
        })
        
        
    except Exception as e:
        logger.error(f"Error getting albums by genre: {str(e)}")
        raise

def get_all_albums(query_params):
    """Get all albums with pagination"""
    try:
        limit = min(int(query_params.get('limit', 20)), 100)
        last_key = query_params.get('lastKey')
        
        table = dynamodb.Table(os.environ['ALBUMS_TABLE'])
        
        scan_params = {
            'Limit': limit,
            'FilterExpression': '#status = :status',
            'ExpressionAttributeNames': {'#status': 'status'},
            'ExpressionAttributeValues': {':status': 'active'}
        }
        
        if last_key:
            scan_params['ExclusiveStartKey'] = decode_last_key(last_key)
        
        response = table.scan(**scan_params)
        
        albums = []
        for item in response.get('Items', []):
            album = transform_album_for_response(item)
            albums.append(album)
        
        # Sort by creation date for consistent ordering
        albums.sort(key=lambda x: x['createdAt'], reverse=True)
        
        logger.info(f"Retrieved {len(albums)} albums")
        
        return create_success_response(200, {
            'message': 'Albums retrieved successfully',
            'albums': albums,
            'count': len(albums),
            'hasMore': 'LastEvaluatedKey' in response,
            'lastKey': encode_last_key(response.get('LastEvaluatedKey')) if 'LastEvaluatedKey' in response else None
        })
        
    except Exception as e:
        logger.error(f"Error getting all albums: {str(e)}")
        raise

def get_album_tracks(album_id):
    """Get tracks for a specific album"""
    try:
        table = dynamodb.Table(os.environ['MUSIC_CONTENT_TABLE'])
        
        # PERFORMANCE: Use albumId-trackNumber-index for ordered track listing
        response = table.query(
            IndexName='albumId-trackNumber-index',
            KeyConditionExpression=Key('albumId').eq(album_id),
            ScanIndexForward=True  # Ascending order by track number
        )
        
        tracks = []
        for item in response.get('Items', []):
            # Convert Decimal to float for JSON serialization
            for key, value in item.items():
                if isinstance(value, Decimal):
                    item[key] = float(value)
            
            track = {
                'contentId': item.get('contentId'),
                'title': item.get('title'),
                'trackNumber': item.get('trackNumber', 0),
                'duration': item.get('duration', 0),
                'fileType': item.get('fileType'),
                'createdAt': item.get('createdAt')
            }
            tracks.append(track)
        
        return tracks
        
    except Exception as e:
        logger.error(f"Error getting album tracks: {str(e)}")
        return []

def transform_album_for_response(item):
    """Transform DynamoDB album item to frontend-friendly format"""
    # Convert Decimal to float for JSON serialization
    for key, value in item.items():
        if isinstance(value, Decimal):
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
        'isExplicit': item.get('isExplicit', False)
    }

def encode_last_key(last_key):
    """Encode last key for pagination"""
    if not last_key:
        return None
    try:
        return base64.b64encode(
            json.dumps(last_key, default=str).encode('utf-8')
        ).decode('utf-8')
    except:
        return None

def decode_last_key(last_key):
    """Decode last key for pagination"""
    if not last_key:
        return None
    try:
        return json.loads(base64.b64decode(last_key).decode('utf-8'))
    except:
        return None

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