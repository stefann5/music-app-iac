import json
import boto3
import os
from datetime import datetime
import logging
import base64
from typing import Dict, List, Any
from boto3.dynamodb.conditions import Key
from decimal import Decimal

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')

def handler(event, context):
    """
    Discover Handler - Optimized for performance with Album support
    
    Supports:
    - GET /discover/genres - List all available genres
    - GET /discover/content?genre=rock - Get content by genre
    - GET /discover/artists?genre=rock - Get artists by genre  
    - GET /discover/albums?genre=rock - Get albums by genre
    - GET /discover/content?genre=rock&artistId=123 - Get content by genre and artist
    - GET /discover/content?genre=rock&albumId=456 - Get content by genre and album
    """
    
    logger.info("Discover request received")
    
    try:
        # Parse request path and query parameters
        path = event.get('path', '')
        query_params = event.get('queryStringParameters') or {}
        
        # Route to appropriate handler based on path
        if path.endswith('/genres'):
            return get_available_genres()
        elif path.endswith('/content'):
            return get_content_by_filters(query_params)
        elif path.endswith('/artists'):
            return get_artists_by_genre(query_params)
        elif path.endswith('/albums'):  # Album discovery
            return get_albums_by_genre(query_params)
        else:
            return create_error_response(400, "Invalid discover endpoint")
            
    except Exception as e:
        logger.error(f"Discover error: {str(e)}")
        return create_error_response(500, "Internal server error")

def get_available_genres():
    """
    Get all available genres from artists, albums, and music content
    PERFORMANCE: Uses parallel queries to all tables for complete coverage
    """
    try:
        # Get genres from all tables in parallel for complete coverage
        music_genres = set()
        artist_genres = set()
        album_genres = set()  # NEW: Include album genres
        
        # PERFORMANCE: Query music content genres using GSI
        music_table = dynamodb.Table(os.environ['MUSIC_CONTENT_TABLE'])
        
        # Use scan with projection to get only genre field (minimal data transfer)
        music_response = music_table.scan(
            ProjectionExpression='genre',
            FilterExpression='attribute_exists(genre) AND genre <> :empty',
            ExpressionAttributeValues={':empty': ''}
        )
        
        for item in music_response.get('Items', []):
            if item.get('genre'):
                music_genres.add(item['genre'].lower().strip())
        
        # PERFORMANCE: Query artist primary genres using GSI  
        artists_table = dynamodb.Table(os.environ['ARTISTS_TABLE'])
        
        artist_response = artists_table.scan(
            ProjectionExpression='primaryGenre, genres',
            FilterExpression='attribute_exists(primaryGenre) AND primaryGenre <> :empty',
            ExpressionAttributeValues={':empty': ''}
        )
        
        for item in artist_response.get('Items', []):
            if item.get('primaryGenre'):
                artist_genres.add(item['primaryGenre'].lower().strip())
            # Also include secondary genres
            for genre in item.get('genres', []):
                if genre and genre.strip():
                    artist_genres.add(genre.lower().strip())
        
        # NEW: Query album genres using GSI
        albums_table = dynamodb.Table(os.environ['ALBUMS_TABLE'])
        
        album_response = albums_table.scan(
            ProjectionExpression='genre',
            FilterExpression='attribute_exists(genre) AND genre <> :empty',
            ExpressionAttributeValues={':empty': ''}
        )
        
        for item in album_response.get('Items', []):
            if item.get('genre'):
                album_genres.add(item['genre'].lower().strip())
        
        # Combine and format genres
        all_genres = sorted(list(music_genres.union(artist_genres).union(album_genres)))
        
        # Format genres with counts for better UX
        genre_data = []
        for genre in all_genres:
            # Get counts for each genre (can be cached in production)
            content_count = get_content_count_by_genre(genre)
            artist_count = get_artist_count_by_genre(genre)
            album_count = get_album_count_by_genre(genre)  # NEW: Album count
            
            genre_data.append({
                'genre': genre.title(),
                'contentCount': content_count,
                'artistCount': artist_count,
                'albumCount': album_count,  # NEW: Include album count
                'totalItems': content_count + artist_count + album_count  # Updated total
            })
        
        # Sort by total items descending for popular genres first
        genre_data.sort(key=lambda x: x['totalItems'], reverse=True)
        
        logger.info(f"Retrieved {len(genre_data)} genres including albums")
        
        return create_success_response(200, {
            'message': 'Genres retrieved successfully',
            'genres': genre_data,
            'count': len(genre_data)
        })
        
    except Exception as e:
        logger.error(f"Error getting genres: {str(e)}")
        raise

def get_content_by_filters(query_params):
    """
    Get content with performance-optimized filtering including album support
    PERFORMANCE: Uses GSI queries instead of scans
    """
    try:
        genre = query_params.get('genre', '').lower().strip()
        artist_id = query_params.get('artistId')
        album_id = query_params.get('albumId')  # Album filtering support
        sort_by = query_params.get('sortBy', 'newest')  # newest, oldest
        limit = min(int(query_params.get('limit', 20)), 100)
        last_key = query_params.get('lastKey')
        
        if not genre:
            return create_error_response(400, "Genre parameter is required")
        
        table = dynamodb.Table(os.environ['MUSIC_CONTENT_TABLE'])
        
        # Choose optimal GSI based on filters
        if album_id:
            # Album-based filtering using albumId-trackNumber-index
            result = query_content_by_album(table, album_id, limit, last_key)
        elif artist_id:
            # PERFORMANCE: Use genre-artistId-index for dual filtering
            result = query_content_by_genre_and_artist(table, genre, artist_id, limit, last_key, sort_by)
        else:
            # PERFORMANCE: Use genre-createdAt-index for chronological content
            result = query_content_by_genre_chronological(table, genre, limit, last_key, sort_by)
        
        logger.info(f"Retrieved {len(result['content'])} content items for genre: {genre}")
        
        return create_success_response(200, result)
        
    except Exception as e:
        logger.error(f"Error getting filtered content: {str(e)}")
        raise

def query_content_by_genre_and_artist(table, genre, artist_id, limit, last_key, sort_by):
    """PERFORMANCE: Query using genre-artistId-index"""
    
    query_params = {
        'IndexName': 'genre-artistId-index',
        'KeyConditionExpression': Key('genre').eq(genre) & Key('artistId').eq(artist_id),
        'Limit': limit,
        'ScanIndexForward': sort_by != 'newest'  # False for newest first
    }
    
    if last_key:
        query_params['ExclusiveStartKey'] = decode_last_key(last_key)
    
    response = table.query(**query_params)
    
    return {
        'message': f'Content retrieved for genre "{genre}" and artist',
        'content': [transform_content_for_response(item) for item in response.get('Items', [])],
        'count': len(response.get('Items', [])),
        'filters': {'genre': genre, 'artistId': artist_id, 'sortBy': sort_by},
        'hasMore': 'LastEvaluatedKey' in response,
        'lastKey': encode_last_key(response.get('LastEvaluatedKey')) if 'LastEvaluatedKey' in response else None
    }

def query_content_by_genre_chronological(table, genre, limit, last_key, sort_by):
    """PERFORMANCE: Query using genre-createdAt-index"""
    
    query_params = {
        'IndexName': 'genre-createdAt-index',
        'KeyConditionExpression': Key('genre').eq(genre),
        'Limit': limit,
        'ScanIndexForward': sort_by != 'newest'  # False for newest first
    }
    
    if last_key:
        query_params['ExclusiveStartKey'] = decode_last_key(last_key)
    
    response = table.query(**query_params)
    
    return {
        'message': f'Content retrieved for genre "{genre}"',
        'content': [transform_content_for_response(item) for item in response.get('Items', [])],
        'count': len(response.get('Items', [])),
        'filters': {'genre': genre, 'sortBy': sort_by},
        'hasMore': 'LastEvaluatedKey' in response,
        'lastKey': encode_last_key(response.get('LastEvaluatedKey')) if 'LastEvaluatedKey' in response else None
    }

def get_artists_by_genre(query_params):
    """
    Get artists by genre using performance-optimized GSI
    PERFORMANCE: Uses primaryGenre-index for fast queries
    """
    try:
        genre = query_params.get('genre', '').lower().strip()
        limit = min(int(query_params.get('limit', 20)), 100)
        last_key = query_params.get('lastKey')
        
        if not genre:
            return create_error_response(400, "Genre parameter is required")
        
        table = dynamodb.Table(os.environ['ARTISTS_TABLE'])
        
        # PERFORMANCE: Use primaryGenre-index for optimal query performance
        query_params = {
            'IndexName': 'primaryGenre-index',
            'KeyConditionExpression': Key('primaryGenre').eq(genre),
            'Limit': limit,
            'ScanIndexForward': True  # Ascending order by name
        }
        
        if last_key:
            query_params['ExclusiveStartKey'] = decode_last_key(last_key)
        
        response = table.query(**query_params)
        
        artists = [transform_artist_for_response(item) for item in response.get('Items', [])]
        
        logger.info(f"Retrieved {len(artists)} artists for genre: {genre}")
        
        return create_success_response(200, {
            'message': f'Artists retrieved for genre "{genre}"',
            'artists': artists,
            'count': len(artists),
            'filters': {'genre': genre},
            'hasMore': 'LastEvaluatedKey' in response,
            'lastKey': encode_last_key(response.get('LastEvaluatedKey')) if 'LastEvaluatedKey' in response else None
        })
        
    except Exception as e:
        logger.error(f"Error getting artists by genre: {str(e)}")
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

def query_content_by_album(table, album_id, limit, last_key):
    """NEW: Query content by album using albumId-trackNumber-index"""
    
    query_params = {
        'IndexName': 'albumId-trackNumber-index',
        'KeyConditionExpression': Key('albumId').eq(album_id),
        'Limit': limit,
        'ScanIndexForward': True  # Ascending order by track number
    }
    
    if last_key:
        query_params['ExclusiveStartKey'] = decode_last_key(last_key)
    
    response = table.query(**query_params)
    
    return {
        'message': f'Content retrieved for album',
        'content': [transform_content_for_response(item) for item in response.get('Items', [])],
        'count': len(response.get('Items', [])),
        'filters': {'albumId': album_id, 'sortBy': 'track_order'},
        'hasMore': 'LastEvaluatedKey' in response,
        'lastKey': encode_last_key(response.get('LastEvaluatedKey')) if 'LastEvaluatedKey' in response else None
    }

# Helper functions for performance optimization

def get_content_count_by_genre(genre):
    """Get count of content items for a genre (can be cached)"""
    try:
        table = dynamodb.Table(os.environ['MUSIC_CONTENT_TABLE'])
        response = table.query(
            IndexName='genre-createdAt-index',
            KeyConditionExpression=Key('genre').eq(genre),
            Select='COUNT'
        )
        return response['Count']
    except:
        return 0

def get_artist_count_by_genre(genre):
    """Get count of artists for a genre (can be cached)"""
    try:
        table = dynamodb.Table(os.environ['ARTISTS_TABLE'])
        response = table.query(
            IndexName='primaryGenre-index',
            KeyConditionExpression=Key('primaryGenre').eq(genre),
            Select='COUNT'
        )
        return response['Count']
    except:
        return 0

def get_album_count_by_genre(genre):
    """NEW: Get count of albums for a genre (can be cached)"""
    try:
        table = dynamodb.Table(os.environ['ALBUMS_TABLE'])
        response = table.query(
            IndexName='genre-createdAt-index',
            KeyConditionExpression=Key('genre').eq(genre),
            Select='COUNT'
        )
        return response['Count']
    except:
        return 0

def transform_content_for_response(item):
    """Transform DynamoDB content item to frontend-friendly format"""
    # Convert Decimal to float for JSON serialization
    for key, value in item.items():
        if isinstance(value, Decimal):
            item[key] = float(value)
    
    return {
        'contentId': item.get('contentId'),
        'title': item.get('title'),
        'artistId': item.get('artistId'),
        'genre': item.get('genre'),
        'album': item.get('album'),
        'albumId': item.get('albumId'),
        'trackNumber': item.get('trackNumber', 0),
        'filename': item.get('filename'),
        'fileType': item.get('fileType'),
        'fileSize': item.get('fileSize'),
        'createdAt': item.get('createdAt'),
        'lastModified': item.get('lastModified'),
        'coverImageUrl': item.get('coverImageUrl')
    }

def transform_artist_for_response(item):
    """Transform DynamoDB artist item to frontend-friendly format"""
    return {
        'artistId': item.get('artistId'),
        'name': item.get('name'),
        'biography': item.get('biography'),
        'primaryGenre': item.get('primaryGenre'),
        'genres': item.get('genres', []),
        'country': item.get('country', ''),
        'formedYear': item.get('formedYear'),
        'imageUrl': item.get('imageUrl', ''),
        'metadata': item.get('metadata', {}),
        'createdAt': item.get('createdAt'),
        'updatedAt': item.get('updatedAt')
    }

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
        'metadata': item.get('metadata', {}),
        'recordLabel': item.get('recordLabel', ''),
        'tags': item.get('tags', []),
        'createdAt': item.get('createdAt'),
        'updatedAt': item.get('updatedAt')
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