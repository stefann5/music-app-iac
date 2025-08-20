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
    Implements requirement 1.3: Kreiranje umetnika (administrator)
    """
    
    logger.info("Create artist request received")
    
    try:
        # Check authorization
        if not is_admin_user(event):
            return create_error_response(403, "Access denied. Administrator role required.")
        
        # Parse request body
        if not event.get('body'):
            return create_error_response(400, "Request body is required")
        
        body = json.loads(event['body'])
        
        # Validate input
        validation_result = validate_artist_input(body)
        if not validation_result['is_valid']:
            return create_error_response(400, "Validation failed", validation_result['errors'])
        
        # Check if artist with same name already exists
        if check_artist_name_exists(body['name']):
            return create_error_response(409, "Artist with this name already exists")
        
        # Create artist
        artist_id = str(uuid.uuid4())
        artist_data = create_artist_record(artist_id, body, event)
        
        # Store in DynamoDB
        store_artist(artist_data)
        
        logger.info(f"Artist created successfully: {artist_id}")
        
        return create_success_response(201, {
            'message': 'Artist created successfully',
            'artist': {
                'artistId': artist_id,
                'name': artist_data['name'],
                'biography': artist_data['biography'],
                'genres': artist_data['genres']
            }
        })
        
    except Exception as e:
        logger.error(f"Create artist error: {str(e)}")
        return create_error_response(500, "Internal server error")

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
        logger.error(f"Error checking admin role: {str(e)}")
        return False

def validate_artist_input(input_data):
    """
    Validate artist creation input according to requirements:
    - name - required, minimum length
    - biography - required, minimum length  
    - genres - required, must be a list with at least one genre
    """
    errors = []
    
    # Name validation
    if not input_data.get('name') or not str(input_data['name']).strip():
        errors.append('Artist name is required')
    elif len(input_data['name'].strip()) < 2:
        errors.append('Artist name must be at least 2 characters')
    
    # Biography validation
    if not input_data.get('biography') or not str(input_data['biography']).strip():
        errors.append('Artist biography is required')
    elif len(input_data['biography'].strip()) < 10:
        errors.append('Artist biography must be at least 10 characters')
    
    # Genres validation
    if not input_data.get('genres'):
        errors.append('At least one genre is required')
    elif not isinstance(input_data['genres'], list):
        errors.append('Genres must be provided as a list')
    elif len(input_data['genres']) == 0:
        errors.append('At least one genre is required')
    else:
        
        for genre in input_data['genres']:
            if not isinstance(genre, str):
                errors.append(f'Invalid genre: {genre}.')
                break
    
    return {
        'is_valid': len(errors) == 0,
        'errors': errors
    }

def check_artist_name_exists(name):
    """Check if artist with the same name already exists"""
    try:
        table = dynamodb.Table(os.environ['ARTISTS_TABLE'])
        response = table.query(
            IndexName='name-index',
            KeyConditionExpression='#name = :name',
            ExpressionAttributeNames={'#name': 'name'},
            ExpressionAttributeValues={':name': name.strip()}
        )
        return len(response['Items']) > 0
    except Exception as e:
        logger.error(f"Error checking artist name: {str(e)}")
        return False

def create_artist_record(artist_id, input_data, event):
    """Create artist record structure"""
    
    # Get creator info from authorizer context
    request_context = event.get('requestContext', {})
    authorizer = request_context.get('authorizer', {})
    creator_username = authorizer.get('username', 'unknown')
    
    return {
        'artistId': artist_id,
        'name': input_data['name'].strip(),
        'biography': input_data['biography'].strip(),
        'genres': [genre.strip().lower() for genre in input_data['genres']],
        'status': 'active',
        'createdAt': datetime.utcnow().isoformat(),
        'updatedAt': datetime.utcnow().isoformat(),
        'createdBy': creator_username,
        'metadata': {
            'totalSongs': 0,
            'totalAlbums': 0,
            'followers': 0,
            'verified': False
        },
        'socialLinks': input_data.get('socialLinks', {}),
        'imageUrl': input_data.get('imageUrl', ''),
        'country': input_data.get('country', ''),
        'formedYear': input_data.get('formedYear'),
        'members': input_data.get('members', [])
    }

def store_artist(artist_data):
    """Store artist data in DynamoDB"""
    try:
        table = dynamodb.Table(os.environ['ARTISTS_TABLE'])
        table.put_item(Item=artist_data)
        logger.info(f"Artist stored successfully: {artist_data['artistId']}")
        
    except Exception as e:
        logger.error(f"Error storing artist: {str(e)}")
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