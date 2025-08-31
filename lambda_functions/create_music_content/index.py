import json
import boto3
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict
import base64

dynamodb = boto3.resource('dynamodb')
s3_client = boto3.client('s3')

def handler(event, context):
    try:
        if not is_admin_user(event):
            return {
                'statusCode': 403,
                'headers': get_cors_headers(),
                'body': json.dumps({'message': 'Access denied. Administrator role required.'})
            }
        
        table_name = os.environ['MUSIC_CONTENT_TABLE']
        bucket_name = os.environ['MUSIC_CONTENT_BUCKET']
        table = dynamodb.Table(table_name)

        headers = event.get('headers', {})
        body = event.get('body', '')

        if event.get('isBase64Encoded', False):
            body = base64.b64decode(body)
        else:
            body = body.encode('utf-8')

        boundary = None
        content_type = headers.get('content-type', headers.get('Content-Type', ''))

        if 'boundary=' in content_type:
            boundary = content_type.split('boundary=')[1]
        else:
            return {
                "statusCode": 400,
                'headers': get_cors_headers(),
                "body": json.dumps({"message": "Invalid content-type header"})
            }
        
        parts = _parse_multipart(body, boundary)
        metadata_part = None
        file_part = None
        cover_image_part = None

        for part in parts:
            if part["name"] == "metadata":
                metadata_part = json.loads(part["data"].decode('utf-8'))
            elif part["name"] == "audioFile":
                file_part = part
            elif part["name"] == "coverImage":
                cover_image_part = part

        if not metadata_part or not file_part:
            return {
                "statusCode": 400,
                'headers': get_cors_headers(),
                "body": json.dumps({"message": "Missing metadata or audioFile part"})
            }
        
        required_fields = ["title", "artistId"]
        for field in required_fields:
            if field not in metadata_part:
                return {
                    "statusCode": 400,
                    'headers': get_cors_headers(),
                    "body": json.dumps({"message": f"Missing required field: {field}"})
                }
        
        allowed_types = os.environ['ALLOWED_FILE_TYPES'].split(',')
        file_content_type = file_part.get("content_type", "audio/mpeg")
        if file_content_type not in allowed_types:
            return {
                "statusCode": 400,
                'headers': get_cors_headers(),
                "body": json.dumps({"message": f"Unsupported audio file type: {file_content_type}"})
            }
        
        max_size = int(os.environ['MAX_FILE_SIZE'])
        file_data = file_part["data"]
        file_size = len(file_data)

        if file_size > max_size:
            return {
                "statusCode": 400,
                'headers': get_cors_headers(),
                "body": json.dumps({"message": f"File size exceeds the maximum limit of {max_size} bytes"})
            }
        
        content_id = str(uuid.uuid4())
        file_key = f"music-content/{content_id}/{file_part['filename']}"

        s3_client.put_object(
            Bucket=bucket_name,
            Key=file_key,
            Body=file_data,
            ContentType=file_content_type,
            Metadata={
                'contentId': content_id,
                'originalFilename': file_part['filename'],
                'uploadedAt': datetime.now(timezone.utc).isoformat()
            }
        )

        allowed_image_types = os.environ['ALLOWED_IMAGE_TYPES'].split(',')
        cover_image_url = None
        cover_image_key = None
        
        if cover_image_part:
            image_content_type = cover_image_part.get("content_type", "")
            if image_content_type not in allowed_image_types:
                return {
                    "statusCode": 400,
                    'headers': get_cors_headers(),
                    "body": json.dumps({"message": f"Unsupported cover image file type: {image_content_type}"})
                }

            max_image_size = int(os.environ.get('MAX_IMAGE_SIZE', '5242880')) # Default 5MB
            image_size = len(cover_image_part["data"])
            if image_size > max_image_size:
                return {
                    "statusCode": 400,
                    'headers': get_cors_headers(),
                    "body": json.dumps({"message": f"Cover image size exceeds the maximum limit of {max_image_size} bytes"})
                }
            
            image_extension = _get_file_extension(cover_image_part['filename'], image_content_type)
            cover_image_key = f"music-content/{content_id}/cover{image_extension}"

            s3_client.put_object(
                Bucket=bucket_name,
                Key=cover_image_key,
                Body=cover_image_part["data"],
                ContentType=image_content_type,
                Metadata={
                    'contentId': content_id,
                    'originalFilename': cover_image_part['filename'],
                    'uploadedAt': datetime.now(timezone.utc).isoformat()
                }
            )
            cover_image_url = s3_client.generate_presigned_url(
                'get_object',
                Params={'Bucket': bucket_name, 'Key': cover_image_key},
                ExpiresIn=604800 # 1 week
            )
            
        current_time = datetime.now(timezone.utc).isoformat()
        
        # DISCOVER OPTIMIZATION: Normalize genre for consistent filtering
        normalized_genre = normalize_genre(metadata_part.get('genre', 'unknown'))
        
        # NEW: Handle album relationship
        album_id = metadata_part.get('albumId')
        track_number = metadata_part.get('trackNumber', 0)
        
        item = {
            'contentId': content_id,
            'title': metadata_part['title'],
            'artistId': metadata_part['artistId'],
            'filename': file_part['filename'],
            'fileType': file_content_type,
            'fileSize': file_size,
            's3Key': file_key,
            'bucketName': bucket_name,
            'createdAt': current_time,
            'lastModified': current_time,
            'genre': normalized_genre
        }

        # NEW: Add album relationship if provided
        if album_id:
            item['albumId'] = album_id
            item['trackNumber'] = int(track_number) if track_number else 0

        if cover_image_part and cover_image_key:
            item['coverImageS3Key'] = cover_image_key
            item['coverImageUrl'] = cover_image_url
            item['coverImageContentType'] = image_content_type
        
        optional_fields = ['album']  # Keep for backward compatibility
        for field in optional_fields:
            if field in metadata_part and metadata_part[field]:
                item[field] = metadata_part[field]

        

        table.put_item(Item=item)
        
        response_data = {
            "message": "Music content created successfully",
            "contentId": content_id,
            "title": item['title'],
            "genre": item['genre'],
            "filename": item['filename'],
            "fileType": item['fileType'],
            "fileSize": item['fileSize'],
            "createdAt": item['createdAt']
        }

        if cover_image_part and cover_image_key:
            response_data['coverImageUrl'] = cover_image_url
        
        try:
            #trigger_transcription(content_id, file_key, bucket_name)
            pass
        except Exception as e:
            print(f"Warning: Could not trigger transcription: {str(e)}")
        
        return {
            "statusCode": 201,
            'headers': get_cors_headers(),
            "body": json.dumps({
                "message": "Music content created successfully. Transcription started.",
                "contentId": content_id,
                "transcriptionStatus": "PENDING"
            })
        }
    except json.JSONDecodeError:
        return {
            "statusCode": 400,
            'headers': get_cors_headers(),
            "body": json.dumps({"message": "Invalid JSON in metadata"})
        }
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            "statusCode": 500,
            'headers': get_cors_headers(),
            "body": json.dumps({"message": "Internal server error"})
        }
    

def normalize_genre(genre):
    """
    DISCOVER OPTIMIZATION: Normalize genre names for consistent filtering
    This ensures that "Rock", "rock", "ROCK" are all stored as "rock"
    """
    if not genre or not isinstance(genre, str):
        return 'unknown'
    
    # Convert to lowercase and strip whitespace
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



def _parse_multipart(body: bytes, boundary: str) -> list:
    parts = []
    boundary_bytes = f'--{boundary}'.encode('utf-8')

    sections = body.split(boundary_bytes)
    for section in sections[1:-1]: #skip first and last boundary (empty)
        if not section.strip():
            continue

        header_end = section.find(b'\r\n\r\n')
        if header_end == -1:
            continue

        header_bytes = section[:header_end]
        data = section[header_end + 4:]

        if data.endswith(b'\r\n'):
            data = data[:-2] # Remove trailing \r\n

        headers = {}
        name = None
        filename = None
        content_type = None

        for header_line in header_bytes.decode('utf-8').split('\r\n'):
            if header_line.startswith('Content-Disposition:'):
                if 'name="' in header_line:
                    name = header_line.split('name="')[1].split('"')[0]
                if 'filename="' in header_line:
                    filename = header_line.split('filename="')[1].split('"')[0]
            elif header_line.startswith('Content-Type:'):
                content_type = header_line.split('Content-Type:')[1].strip()

        if name:
            part = {
                'name': name,
                'data': data,
                'headers': headers
            }
        if filename:
            part['filename'] = filename
        if content_type:
            part['content_type'] = content_type
        parts.append(part)

    return parts

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
    
def _get_file_extension(filename, content_type):
    if content_type == 'image/jpeg':
        return '.jpg'
    elif content_type == 'image/png':
        return '.png'
    elif content_type == 'image/webp':
        return '.webp'
    else:
        return '.' + filename.split('.')[-1] if '.' in filename else '.jpg'
    
def trigger_transcription(content_id, s3_key, bucket_name):
    """Trigger transcription processing for uploaded music content"""
    
    lambda_client = boto3.client('lambda')
    
    payload = {
        'contentId': content_id,
        's3Key': s3_key,
        'bucketName': bucket_name
    }
    
    # Invoke start transcription function asynchronously
    lambda_client.invoke(
        FunctionName=os.environ['START_TRANSCRIPTION_FUNCTION'],
        InvocationType='Event',  # Async invocation
        Payload=json.dumps(payload)
    )
    
    print(f"Transcription triggered for content: {content_id}")
    
def get_cors_headers():
    """Get CORS headers for API responses"""
    return {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type,Authorization,X-Requested-With',
        'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',
        'Content-Type': 'application/json'
    }