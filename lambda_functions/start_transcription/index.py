import json
import boto3
import uuid
import os
from datetime import datetime
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

transcribe_client = boto3.client('transcribe')
dynamodb = boto3.resource('dynamodb')
sqs = boto3.client('sqs')

def handler(event, context):
    """
    Start Transcription Handler - FIXED with duplicate prevention
    """
    
    logger.info(f"Start transcription event: {json.dumps(event)}")
    
    try:
        # Parse the trigger event
        content_id = event.get('contentId')
        s3_key = event.get('s3Key')
        bucket_name = event.get('bucketName')
        
        if not all([content_id, s3_key, bucket_name]):
            raise ValueError(f"Missing required parameters: contentId={content_id}, s3Key={s3_key}, bucketName={bucket_name}")
        
        # CHECK FOR EXISTING TRANSCRIPTION FIRST (prevent duplicates)
        existing_transcription = check_existing_transcription(content_id)
        if existing_transcription:
            logger.info(f"Transcription already exists for content {content_id}: {existing_transcription['status']}")
            
            # If it's failed and retries are available, continue with retry
            if existing_transcription['status'] == 'FAILED' and existing_transcription.get('retryCount', 0) < 3:
                logger.info(f"Retrying failed transcription: {existing_transcription['transcriptionId']}")
                transcription_id = existing_transcription['transcriptionId']
            else:
                # Skip if already exists and not failed or out of retries
                return {
                    'statusCode': 200,
                    'body': json.dumps({
                        'message': 'Transcription already exists or processing',
                        'transcriptionId': existing_transcription['transcriptionId'],
                        'status': existing_transcription['status']
                    })
                }
        else:
            # Create new transcription record
            transcription_id = str(uuid.uuid4())
        
        transcription_record = create_transcription_record(
            transcription_id, 
            content_id, 
            s3_key, 
            bucket_name
        )
        
        # Store/update in DynamoDB
        store_transcription_record(transcription_record)
        
        # Start Amazon Transcribe job
        job_name = f"transcription-{transcription_id}"
        media_uri = f"s3://{bucket_name}/{s3_key}"
        
        logger.info(f"Starting transcription job: {job_name}")
        logger.info(f"Media URI: {media_uri}")
        
        transcribe_response = transcribe_client.start_transcription_job(
            TranscriptionJobName=job_name,
            Media={'MediaFileUri': media_uri},
            MediaFormat=get_audio_format(s3_key),
            LanguageCode='en-US',  # Serbian language
            Settings={
                'ShowSpeakerLabels': False,
                'ShowAlternatives': True,
                'MaxAlternatives': 2
            },
            OutputBucketName=bucket_name,
            OutputKey=f"transcriptions/{transcription_id}/result.json"
        )
        
        logger.info(f"Transcribe job started: {transcribe_response['TranscriptionJob']['TranscriptionJobName']}")
        
        # Update status to PROCESSING
        update_transcription_status(transcription_id, 'PROCESSING', {
            'jobName': job_name,
            'startedAt': datetime.utcnow().isoformat()
        })
        
        # Send message to SQS for monitoring
        send_monitoring_message(transcription_id, job_name)
        
        logger.info(f"Transcription started successfully: {job_name}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Transcription started successfully',
                'transcriptionId': transcription_id,
                'jobName': job_name
            })
        }
        
    except Exception as e:
        logger.error(f"Error starting transcription: {str(e)}")
        
        # Update status to FAILED if we have transcription_id
        if 'transcription_id' in locals():
            update_transcription_status(transcription_id, 'FAILED', {
                'error': str(e),
                'failedAt': datetime.utcnow().isoformat()
            })
        
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Failed to start transcription'})
        }

def check_existing_transcription(content_id):
    """Check if transcription already exists for this content"""
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
        logger.error(f"Error checking existing transcription: {str(e)}")
        return None

def create_transcription_record(transcription_id, content_id, s3_key, bucket_name):
    """Create initial transcription record"""
    return {
        'transcriptionId': transcription_id,
        'contentId': content_id,
        's3Key': s3_key,
        'bucketName': bucket_name,
        'status': 'PROCESSING',  # Set to PROCESSING immediately
        'createdAt': datetime.utcnow().isoformat(),
        'updatedAt': datetime.utcnow().isoformat(),
        'retryCount': 0,
        'metadata': {
            'language': 'en-US',
            'confidence': None,
            'duration': None
        }
    }

def store_transcription_record(record):
    """Store transcription record in DynamoDB"""
    table = dynamodb.Table(os.environ['TRANSCRIPTIONS_TABLE'])
    table.put_item(Item=record)
    logger.info(f"Transcription record stored: {record['transcriptionId']}")

def update_transcription_status(transcription_id, status, additional_data=None):
    """Update transcription status"""
    table = dynamodb.Table(os.environ['TRANSCRIPTIONS_TABLE'])
    
    update_expression = "SET #status = :status, updatedAt = :updated_at"
    expression_values = {
        ':status': status,
        ':updated_at': datetime.utcnow().isoformat()
    }
    expression_names = {'#status': 'status'}
    
    if additional_data:
        for key, value in additional_data.items():
            update_expression += f", {key} = :{key}"
            expression_values[f':{key}'] = value
    
    table.update_item(
        Key={'transcriptionId': transcription_id},
        UpdateExpression=update_expression,
        ExpressionAttributeValues=expression_values,
        ExpressionAttributeNames=expression_names
    )
    
    logger.info(f"Transcription status updated: {transcription_id} -> {status}")

def get_audio_format(s3_key):
    """Determine audio format from file extension"""
    extension = s3_key.lower().split('.')[-1]
    format_mapping = {
        'mp3': 'mp3',
        'wav': 'wav',
        'flac': 'flac',
        'm4a': 'mp4',
        'ogg': 'ogg'
    }
    return format_mapping.get(extension, 'mp3')

def send_monitoring_message(transcription_id, job_name):
    """Send message to SQS for transcription monitoring"""
    try:
        message = {
            'transcriptionId': transcription_id,
            'jobName': job_name,
            'action': 'monitor',
            'timestamp': datetime.utcnow().isoformat()
        }
        
        response = sqs.send_message(
            QueueUrl=os.environ['TRANSCRIPTION_QUEUE_URL'],
            MessageBody=json.dumps(message),
            DelaySeconds=60  # Check after 1 minute
        )
        
        logger.info(f"Monitoring message sent: {response['MessageId']}")
        
    except Exception as e:
        logger.error(f"Error sending monitoring message: {str(e)}")
        raise