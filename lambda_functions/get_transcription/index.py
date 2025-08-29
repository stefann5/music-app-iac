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
    Get Transcription Handler
    Returns transcription text for a music content
    """
    
    try:
        # Get contentId from query parameters
        query_params = event.get('queryStringParameters') or {}
        content_id = query_params.get('contentId')
        
        if not content_id:
            return create_error_response(400, "contentId parameter is required")
        
        # Get transcription from DynamoDB
        transcription = get_transcription_by_content_id(content_id)
        
        if not transcription:
            return create_success_response(200, {
                'contentId': content_id,
                'status': 'NOT_FOUND',
                'message': 'No transcription found for this content'
            })
        
        # Return transcription data
        response_data = {
            'contentId': content_id,
            'transcriptionId': transcription['transcriptionId'],
            'status': transcription['status'],
            'createdAt': transcription['createdAt'],
            'updatedAt': transcription['updatedAt']
        }
        
        if transcription['status'] == 'COMPLETED':
            response_data.update({
                'text': transcription['transcriptionText'],
                'confidence': transcription.get('confidence', 0),
                'wordCount': transcription.get('wordCount', 0),
                'completedAt': transcription.get('completedAt')
            })
        elif transcription['status'] == 'PROCESSING':
            response_data['message'] = 'Transcription is still being processed'
        elif transcription['status'] == 'FAILED':
            response_data['message'] = 'Transcription failed'
            
        return create_success_response(200, response_data)
        
    except Exception as e:
        logger.error(f"Error getting transcription: {str(e)}")
        return create_error_response(500, "Internal server error")

def get_transcription_by_content_id(content_id):
    """Get transcription record by contentId"""
    try:
        table = dynamodb.Table(os.environ['TRANSCRIPTIONS_TABLE'])
        
        response = table.query(
            IndexName='contentId-index',
            KeyConditionExpression='contentId = :content_id',
            ExpressionAttributeValues={':content_id': content_id},
            ScanIndexForward=False,  # Get latest first
            Limit=1
        )
        
        if response['Items']:
            return response['Items'][0]
        return None
        
    except Exception as e:
        logger.error(f"Error querying transcription: {str(e)}")
        return None

def create_success_response(status_code, data):
    return {
        'statusCode': status_code,
        'headers': get_cors_headers(),
        'body': json.dumps(data, default=str)
    }

def create_error_response(status_code, message):
    return {
        'statusCode': status_code,
        'headers': get_cors_headers(),
        'body': json.dumps({
            'error': message,
            'timestamp': datetime.utcnow().isoformat()
        })
    }

def get_cors_headers():
    return {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type,Authorization,X-Requested-With',
        'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',
        'Content-Type': 'application/json'
    }