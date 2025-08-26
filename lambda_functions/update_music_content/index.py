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
        table = dynamodb.Table(table_name)

        body = json.loads(event['body']) if event.get('body') else {}

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
                    "content": updated_item
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