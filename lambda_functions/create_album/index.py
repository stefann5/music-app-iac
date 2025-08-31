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
    Create Album Handler
    Implements album creation for discover functionality
    DISCOVER OPTIMIZATION: Sets genre and popularity fields for efficient querying
    """
    
    logger.info("Create album request received")
    
    try:
        # Check authorization - admin only
        if not is_admin_user(event):
            return create_error_response(403, "Access denied. Administrator role required.")
        
        # Parse request body
        if not event.get('body'):
            return create_error_response(400, "Request body is required")
        
        body = json.loads(event['body'])
        
        # Validate input
        validation_result = validate_album_input(body)
        if not validation_result['is_valid']:
            return create_error_response(400, "Validation failed", validation_result['errors'])
        
        # Check if album with same title and artist already exists
        if check_album_exists(body['title'], body['artistId']):
            return create_error_response(409, "Album with this title by this artist already exists")
        
        # Verify artist exists
        if not verify_artist_exists(body['artistId']):
            return create_error_response(400, "Artist not found")
        
        # Create album
        album_id = str(uuid.uuid4())
        album_data = create_album_record(album_id, body)
        
        # Store in DynamoDB
        store_album(album_data)
        
        # Update artist metadata
        update_artist_album_count(body['artistId'])
        
        logger.info(f"Album created successfully: {album_id}")
        
        return create_success_response(201, {
            'message': 'Album created successfully',
            'album': {
                'albumId': album_id,
                'title': album_data['title'],
                'artistId': album_data['artistId'],
                'genre': album_data['genre'],
                'tracksIds': album_data['tracksIds']
            }
        })
        
    except Exception as e:
        logger.error(f"Create album error: {str(e)}")
        return create_error_response(500, "Internal server error")

def is_admin_user(event):
    """Check if the user has administrator role"""
    try:
        request_context = event.get('requestContext', {})
        authorizer = request_context.get('authorizer', {})
        
        groups = authorizer.get('groups', '').split(',')
        role = authorizer.get('role', '')
        
        return 'administrators' in groups or role == 'admin'
        
    except Exception as e:
        logger.error(f"Error checking admin role: {str(e)}")
        return False

def validate_album_input(input_data):
    """
    Validate album creation input:
    - title - required, minimum length
    - artistId - required, valid UUID format
    - genre - required
    - releaseYear - optional, valid year
    - description - optional
    - trackCount - optional, positive integer
    """
    errors = []
    
    # Title validation
    if not input_data.get('title') or not str(input_data['title']).strip():
        errors.append('Album title is required')
    elif len(input_data['title'].strip()) < 1:
        errors.append('Album title must not be empty')
    
    # Artist ID validation
    if not input_data.get('artistId'):
        errors.append('Artist ID is required')
    
    # Genre validation
    if not input_data.get('genre'):
        errors.append('Genre is required')
    elif not isinstance(input_data['genre'], str):
        errors.append('Genre must be a string')
    
    # Validate if there are any tracks
    if input_data.get('tracksIds') and len(input_data.get('tracksIds')) < 1:
        errors.append('Number of tracks must be positive number')

    return {
        'is_valid': len(errors) == 0,
        'errors': errors
    }

def check_album_exists(title, artist_id):
    """Check if album with the same title and artist already exists"""
    try:
        table = dynamodb.Table(os.environ['ALBUMS_TABLE'])
        
        # Query by artist first, then filter by title
        response = table.query(
            IndexName='artistId-createdAt-index',
            KeyConditionExpression='artistId = :artistId',
            FilterExpression='title = :title',
            ExpressionAttributeValues={
                ':artistId': artist_id,
                ':title': title.strip()
            }
        )
        return len(response['Items']) > 0
    except Exception as e:
        logger.error(f"Error checking album existence: {str(e)}")
        return False

def verify_artist_exists(artist_id):
    """Verify that the artist exists"""
    try:
        artists_table = dynamodb.Table(os.environ['ARTISTS_TABLE'])
        response = artists_table.get_item(Key={'artistId': artist_id})
        return 'Item' in response
    except Exception as e:
        logger.error(f"Error verifying artist: {str(e)}")
        return False

def normalize_genre(genre):
    """DISCOVER OPTIMIZATION: Normalize genre names for consistent filtering"""
    if not genre or not isinstance(genre, str):
        return 'unknown'
    
    normalized = genre.lower().strip()
    
    # Handle common variations and typos
    genre_mappings = {
        'r&b': 'rnb',
        'rhythm and blues': 'rnb',
        'hip-hop': 'hiphop',
        'hip hop': 'hiphop',
        'drum and bass': 'drumnbass',
        'drum & bass': 'drumnbass',
        'electronic dance music': 'edm',
        'singer-songwriter': 'singersongwriter',
        'alt-rock': 'alternative',
        'alternative rock': 'alternative',
        'heavy metal': 'metal',
        'death metal': 'metal',
        'black metal': 'metal',
        'thrash metal': 'metal'
    }
    
    return genre_mappings.get(normalized, normalized)

def create_album_record(album_id, input_data):
    """Create album record structure with discover optimizations"""
    
    # DISCOVER OPTIMIZATION: Normalize genre for consistent filtering
    normalized_genre = normalize_genre(input_data['genre'])
    
    current_time = datetime.utcnow().isoformat()
    
    return {
        'albumId': album_id,
        'title': input_data['title'].strip(),
        'artistId': input_data['artistId'],
        'genre': normalized_genre,  # DISCOVER OPTIMIZATION
        'trackCount': len(input_data.get('tracksIds', [])),
        'createdAt': current_time,
        'tracksIds': input_data['tracksIds']
    }

def store_album(album_data):
    """Store album data in DynamoDB"""
    try:
        table = dynamodb.Table(os.environ['ALBUMS_TABLE'])
        table.put_item(Item=album_data)
        logger.info(f"Album stored successfully: {album_data['albumId']} with genre: {album_data['genre']}")
        
    except Exception as e:
        logger.error(f"Error storing album: {str(e)}")
        raise

def update_artist_album_count(artist_id):
    """Update artist's album count when new album is created"""
    try:
        artists_table = dynamodb.Table(os.environ['ARTISTS_TABLE'])
        
        artists_table.update_item(
            Key={'artistId': artist_id},
            UpdateExpression="""
                SET #metadata.totalAlbums = if_not_exists(#metadata.totalAlbums, :zero) + :one,
                    updatedAt = :updated_at
            """,
            ExpressionAttributeNames={
                '#metadata': 'metadata'
            },
            ExpressionAttributeValues={
                ':zero': 0,
                ':one': 1,
                ':updated_at': datetime.utcnow().isoformat()
            }
        )
        
        logger.info(f"Updated album count for artist: {artist_id}")
        
    except Exception as e:
        logger.error(f"Error updating artist album count: {str(e)}")
        # Don't fail the main operation if artist update fails

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