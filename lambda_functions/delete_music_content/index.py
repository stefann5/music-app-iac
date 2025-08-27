import json
import boto3
import os

dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')

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

        content_id = None
        if event.get('queryStringParameters') and event['queryStringParameters']:
            content_id = event['queryStringParameters'].get('contentId')

        if not content_id and event.get('body'):
            try:
                body = json.loads(event['body'])
                content_id = body.get('contentId')
            except json.JSONDecodeError:
                return {
                    'statusCode': 400,
                    'headers': get_cors_headers(),
                    'body': json.dumps({'message': 'Invalid JSON in request body'})
                }
        if not content_id:
            return {
                'statusCode': 400,
                'headers': get_cors_headers(),
                'body': json.dumps({'message': 'contentId is required in query parameters or body'})
            }
        
        try:
            response = table.get_item(Key={'contentId': content_id})
            if 'Item' not in response:
                return {
                    'statusCode': 404,
                    'headers': get_cors_headers(),
                    'body': json.dumps({'message': 'Content not found'})
                }
            item = response['Item']
            s3_key = item.get('s3Key')
            image_s3_key = item.get('imageS3Key')
            title = item.get('title', 'Unknown')
        except Exception as e:
            return {
                'statusCode': 500,
                'headers': get_cors_headers(),
                'body': json.dumps({'message': f'Error retrieving item from DynamoDB: {str(e)}'})
            }
        #Delete from S3 audio file
        s3_deleted = False
        if s3_key:
            try:
                s3.delete_object(Bucket=bucket_name, Key=s3_key)
                s3_deleted = True
                print(f"S3 object {s3_key} deleted successfully.")
            except Exception as e:
                print(f"Error deleting S3 object {s3_key}: {str(e)}")
        #Delete from S3 cover image file
        if image_s3_key:
            try:
                s3.delete_object(Bucket=bucket_name, Key=image_s3_key)
                print(f"S3 object {image_s3_key} deleted successfully.")
            except Exception as e:
                print(f"Error deleting S3 object {image_s3_key}: {str(e)}")
        #Delete from DynamoDB
        try:
            table.delete_item(Key={'contentId': content_id})
            print(f"DynamoDB item with contentId {content_id} deleted successfully.")
            return {
                'statusCode': 200,
                'headers': get_cors_headers(),
                'body': json.dumps({
                    'message': f'Content "{title}" deleted successfully.',
                })
            }
        except Exception as e:
            print(f"Error deleting item from DynamoDB: {str(e)}")
            return {
                'statusCode': 500,
                'headers': get_cors_headers(),
                'body': json.dumps({'message': f'Failed to delete item from DynamoDB: {str(e)}', 's3_deleted': s3_deleted})
            }
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        return {
            'statusCode': 500,
            'headers': get_cors_headers(),
            'body': json.dumps({'message': f'Internal server error: {str(e)}'})
        }
    
def is_admin_user(event):
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
    
def get_cors_headers():
    """Get CORS headers for API responses"""
    return {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type,Authorization,X-Requested-With',
        'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',
        'Content-Type': 'application/json'
    }