import boto3
import os
from datetime import datetime, timezone
from typing import Any, Dict
from decimal import Decimal
import base64
import uuid
import json

dynamodb = boto3.resource('dynamodb')
s3_client = boto3.client('s3')

def handler(event, context):
    try:
        if not is_admin_user(event):
            return {
                'statusCode': 403,
                'body': json.dumps({'message': 'Access denied. Administrator role required.'})
            }
        
        table_name = os.environ['MUSIC_CONTENT_TABLE']
        bucket_name = os.environ['MUSIC_CONTENT_BUCKET']
        table = dynamodb.Table(table_name)

        headers = event.get('headers', {})
        content_type = headers.get('Content-Type') or headers.get('content-type', '')
        # Determine if the request is multipart/form-data
        is_multipart = 'multipart/form-data' in content_type
        if is_multipart:
            return _handle_mutipart_update(event, table, bucket_name)
        else:
            return _handle_json_update(event, table)
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({"message": "Internal server error"})
        }

def _handle_json_update(event, table):
    try:
        body = json.loads(event['body']) if event.get('body') else {}
        # Extract contentId from query parameters or body
        content_id = None
        if event.get('queryStringParameters') and event['queryStringParameters']:
            content_id = event['queryStringParameters'].get('contentId')
        if not content_id and 'contentId' in body:
            content_id = body['contentId']

        if not content_id:
            return {
                "statusCode": 400,
                "body": json.dumps({"message": "Missing contentId"})
            }
        # Fetch existing item to ensure it exists
        try:
            response = table.get_item(Key={'contentId': content_id})

            if 'Item' not in response:
                return {
                    "statusCode": 404,
                    "body": json.dumps({"message": "Content not found"})
                }
        except Exception as e:
            print(f"Error fetching existing item: {str(e)}")
            return {
                "statusCode": 500,
                "body": json.dumps({"message": "Error fetching existing content"})
            }
        
        update_expression_parts = []
        expression_attribute_values = {}
        expression_attribute_names = {}

        updatable_fields = ['title', 'album', 'genre', 'coverImage']
        # Dynamically build the update expression
        for field in updatable_fields:
            if field in body and body[field] is not None:
                update_expression_parts.append(f"#{field} = :{field}")
                expression_attribute_values[f":{field}"] = body[field]
                expression_attribute_names[f"#{field}"] = field

        if update_expression_parts:
            update_expression_parts.append("#lastModified = :lastModified")
            expression_attribute_values[":lastModified"] = datetime.now(timezone.utc).isoformat()
            expression_attribute_names["#lastModified"] = "lastModified"

        if not update_expression_parts:
            return {
                "statusCode": 400,
                "body": json.dumps({"message": "No valid fields provided to update", "updatableFields": updatable_fields})
            }
        # Perform the update
        update_expression = "SET " + ", ".join(update_expression_parts)

        try:
            response = table.update_item(
                Key={'contentId': content_id},
                UpdateExpression=update_expression,
                ExpressionAttributeValues=expression_attribute_values,
                ExpressionAttributeNames=expression_attribute_names,
                ReturnValues="ALL_NEW"
            )
            updated_item = response["Attributes"]
            updated_item = json.loads(json.dumps(updated_item, default=decimal_converter))
            print(f"Succcessfully updated item: {content_id}")
            return {
                "statusCode": 200,
                "body": json.dumps({
                    "message": "Music content updated successfully",
                    "content": _sanitize_item(updated_item)
                })
            }
        except Exception as e:
            print(f"Error updating item: {str(e)}")
            return {
                "statusCode": 500,
                "body": json.dumps({"message": "Error updating content"})
            }
    except json.JSONDecodeError:
        return {
            "statusCode": 400,
            "body": json.dumps({"message": "Invalid JSON in request body"})
        }
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({"message": "Internal server error"})
        }

def _handle_mutipart_update(event, table, bucket_name):
    try:
        # Extracting boundary and body
        headers = event.get('headers', {})
        body = event.get('body', '')
        # Decode body if it's base64 encoded
        if event.get('isBase64Encoded', False):
            body = base64.b64decode(body)
        else:
            body = body.encode('utf-8')
        
        content_type = headers.get('Content-Type') or headers.get('content-type', '')
        if 'boundary=' not in content_type:
            return {
                "statusCode": 400,
                "body": json.dumps({"message": "Invalid Content-Type header"})
            }

        boundary = content_type.split('boundary=')[1]
        parts = _parse_multipart(body, boundary)
        # Extracting parts
        metadata_part = None
        audio_file_part = None
        cover_image_part = None

        for part in parts:
            if part['name'] == 'metadata':
                metadata_part = json.loads(part['data'].decode('utf-8'))
            elif part['name'] == 'audioFile':
                audio_file_part = part
            elif part['name'] == 'coverImage':
                cover_image_part = part

        if not metadata_part or 'contentId' not in metadata_part:
            return {
                "statusCode": 400,
                "body": json.dumps({"message": "Missing metadata or contentId"})
            }
        
        content_id = metadata_part['contentId']
        response = table.get_item(Key={'contentId': content_id})
        if 'Item' not in response:
            return {
                "statusCode": 404,
                "body": json.dumps({"message": "Content not found"})
            }
        
        existing_item = response['Item']
        files_updated = []
        # Handling audio file update
        if audio_file_part:
            result = _update_audio_file(audio_file_part, existing_item, bucket_name)
            if result['error']:
                return {
                    "statusCode": 400,
                    "body": json.dumps({"message": result['error']})
                }
            files_updated.append('audio')
        # Handling cover image update
        if cover_image_part:
            result = _update_cover_image(cover_image_part, existing_item, bucket_name)
            if result['error']:
                return {
                    "statusCode": 400,
                    "body": json.dumps({"message": result['error']})
                }
            files_updated.append('coverImage')

        update_expression_parts = []
        expression_attribute_values = {}
        expression_attribute_names = {}
        # Fields that can be updated via metadata
        updatable_fields = ['title', 'album', 'genre']
        for field in updatable_fields:
            if field in metadata_part and metadata_part[field] is not None:
                update_expression_parts.append(f"#{field} = :{field}")
                expression_attribute_values[f":{field}"] = metadata_part[field]
                expression_attribute_names[f"#{field}"] = field
        # If audio file was updated, update related fields
        if audio_file_part:
            update_expression_parts.extend([
                "#filename = :filename",
                "#fileType = :fileType",
                "#fileSize = :fileSize",
                "#s3Key = :s3Key"
            ])
            expression_attribute_values.update({
                ":filename": audio_file_part['filename'],
                ":fileType": audio_file_part.get('content_type', 'audio/mpeg'),
                ":fileSize": len(audio_file_part['data']),
                ":s3Key": f"music-content/{content_id}/audio/{audio_file_part['filename']}"
            })
            expression_attribute_names.update({
                "#filename": "filename",
                "#fileType": "fileType",
                "#fileSize": "fileSize",
                "#s3Key": "s3Key"
            })
        # If cover image was updated, update related fields
        if cover_image_part:
            image_extension = _get_file_extension(cover_image_part['filename'], cover_image_part.get('content_type', 'image/jpeg'))
            cover_image_s3_key = f"music-content/{content_id}/cover{image_extension}"

            cover_image_url = s3_client.generate_presigned_url(
                'get_object',
                Params={'Bucket': bucket_name, 'Key': cover_image_s3_key},
                ExpiresIn=31536000  # 1 year
            )

            update_expression_parts.extend([
                "#coverImageS3Key = :coverImageS3Key",
                "#coverImageUrl = :coverImageUrl",
                "#coverImageContentType = :coverImageContentType"
            ])
            expression_attribute_values.update({
                ":coverImageS3Key": cover_image_s3_key,
                ":coverImageUrl": cover_image_url,
                ":coverImageContentType": cover_image_part.get('content_type', 'image/jpeg')
            })
            expression_attribute_names.update({
                "#coverImageS3Key": "coverImageS3Key",
                "#coverImageUrl": "coverImageUrl",
                "#coverImageContentType": "coverImageContentType"
            })

        update_expression_parts.append("#lastModified = :lastModified")
        expression_attribute_values[":lastModified"] = datetime.now(timezone.utc).isoformat()
        expression_attribute_names["#lastModified"] = "lastModified"

        if not update_expression_parts:
            return {
                "statusCode": 400,
                "body": json.dumps({"message": "No valid fields or files provided to update"})
            }
        # Performing the update
        update_expression = "SET " + ", ".join(update_expression_parts)

        response = table.update_item(
            Key={'contentId': content_id},
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_attribute_values,
            ExpressionAttributeNames=expression_attribute_names,
            ReturnValues="ALL_NEW"
        )

        updated_item = response["Attributes"]
        updated_item = json.loads(json.dumps(updated_item, default=decimal_converter))

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Music content updated successfully",
                "content": _sanitize_item(updated_item),
                "filesUpdated": files_updated
            })
        }
    except json.JSONDecodeError:
        return {
            "statusCode": 400,
            "body": json.dumps({"message": "Invalid JSON in metadata"})
        }
    except Exception as e:
        print(f"Error in multipart update: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({"message": "Error updating multipart content"})
        }

def _update_audio_file(part, existing_item, bucket_name):
    try:
        # Validation for audio file type
        allowed_file_types = os.environ.get('ALLOWED_FILE_TYPES').split(',')
        audio_content_type = part.get('content_type', 'audio/mpeg')

        if audio_content_type not in allowed_file_types:
            return {'error': f"Invalid audio file type. Allowed types: {', '.join(allowed_file_types)}"}
        # Validation for audio file size
        max_audio_size = int(os.environ.get('MAX_FILE_SIZE', '10485760'))  # 10 MB default
        audio_size = len(part['data'])
        if audio_size > max_audio_size:
            return {'error': f"Audio file size exceeds the maximum limit of {max_audio_size} bytes"}
        
        content_id = existing_item['contentId']
        new_s3_key = f"music-content/{content_id}/audio/{part['filename']}"
        # Deleting old audio file from S3 if exists
        old_s3_key = existing_item.get('s3Key')
        if old_s3_key:
            try:
                s3_client.delete_object(Bucket=bucket_name, Key=old_s3_key)
                print(f"Deleted old audio file from S3: {old_s3_key}")
            except Exception as e:
                print(f"Error deleting old audio file from S3: {str(e)}")
        # Uploading new audio file to S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key=new_s3_key,
            Body=part['data'],
            ContentType=audio_content_type,
            Metadata={
                'contentId': content_id,
                'originalFilename': part['filename'],
                'uploadedAt': datetime.now(timezone.utc).isoformat(),
                'fileType': 'audio'
            }
        )

        print(f"Uploaded new audio file to S3: {new_s3_key}")
        return {'error': None}
    except Exception as e:
        print(f"Error updating audio file: {str(e)}")
        return {'error': "Error uploading audio file"}

def _update_cover_image(part, existing_item, bucket_name):
    try:
        #Validation for image file type
        allowed_image_types = os.environ.get('ALLOWED_IMAGE_TYPES').split(',')
        image_content_type = part.get('content_type', 'image/jpeg')

        if image_content_type not in allowed_image_types:
            return {'error': f"Invalid image file type. Allowed types: {', '.join(allowed_image_types)}"}
        #Validation for image file size
        max_image_size = int(os.environ.get('MAX_IMAGE_SIZE', '5242880'))  # 5 MB default
        image_size = len(part['data'])
        if image_size > max_image_size:
            return {'error': f"Image file size exceeds the maximum limit of {max_image_size} bytes"}
        
        content_id = existing_item['contentId']
        image_extension = _get_file_extension(part['filename'], image_content_type)
        new_s3_key = f"music-content/{content_id}/cover{image_extension}"
        # Deleting old cover image from S3 if exists
        old_s3_key = existing_item.get('coverImageS3Key')
        if old_s3_key:
            try:
                s3_client.delete_object(Bucket=bucket_name, Key=old_s3_key)
                print(f"Deleted old cover image from S3: {old_s3_key}")
            except Exception as e:
                print(f"Error deleting old cover image from S3: {str(e)}")
        # Uploading new cover image to S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key=new_s3_key,
            Body=part['data'],
            ContentType=image_content_type,
            Metadata={
                'contentId': content_id,
                'originalFilename': part['filename'],
                'uploadedAt': datetime.now(timezone.utc).isoformat(),
                'fileType': 'image'
            }
        )

        print(f"Uploaded new cover image to S3: {new_s3_key}")
        return {'error': None}
    except Exception as e:
        print(f"Error updating cover image: {str(e)}")
        return {'error': "Error uploading cover image"}

def _get_file_extension(filename, content_type):
    if content_type == 'image/jpeg':
        return '.jpg'
    elif content_type == 'image/png':
        return '.png'
    elif content_type == 'image/webp':
        return '.webp'
    else:
        return '.' + filename.split('.')[-1] if '.' in filename else '.jpg'

def _parse_multipart(body, boundary):
    parts = []
    boundary_bytes = f'--{boundary}'.encode('utf-8')

    sections = body.split(boundary_bytes)
    for section in sections[1:-1]:
        if not section.strip():
            continue

        header_end = section.find(b'\r\n\r\n')
        if header_end == -1:
            continue

        header_bytes = section[:header_end]
        data = section[header_end + 4:]

        if data.endswith(b'\r\n'):
            data = data[:-2]

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
                'data': data
            }
            if filename:
                part['filename'] = filename
            if content_type:
                part['content_type'] = content_type
            parts.append(part)

    return parts

def decimal_converter(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")

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
    
#Removes sensitive fields from DynamoDB item before returning to client
def _sanitize_item(item):
    safe_item = dict(item)
    #Remove sensitive fields
    safe_fields_to_remove = ['bucketName', 's3Key', 'coverImageS3Key']
    for field in safe_fields_to_remove:
        safe_item.pop(field, None)
    
    return safe_item