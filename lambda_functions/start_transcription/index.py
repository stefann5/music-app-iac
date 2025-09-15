import json
import boto3
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
    Start Transcription Handler - Updated to use contentId as primary key
    Simplified logic since contentId is now the key
    """
    
    logger.info(f"Start transcription event: {json.dumps(event)}")
    
    try:
        # Parse the trigger event
        content_id = event.get('contentId')
        s3_key = event.get('s3Key')
        bucket_name = event.get('bucketName')
        
        logger.info(f"contentId: {content_id}")
        
        if not all([content_id, s3_key, bucket_name]):
            raise ValueError(f"Missing required parameters: contentId={content_id}, s3Key={s3_key}, bucketName={bucket_name}")
        
        # CHECK FOR EXISTING TRANSCRIPTION FIRST (prevent duplicates)
        existing_transcription = check_existing_transcription(content_id)
        if existing_transcription:
            logger.info(f"Transcription already exists for content {content_id}: {existing_transcription['status']}")
            
            # If it's failed and retries are available, continue with retry
            if existing_transcription['status'] == 'FAILED' and existing_transcription.get('retryCount', 0) < 3:
                logger.info(f"Retrying failed transcription for content: {content_id}")
                # Continue with retry logic below
            elif existing_transcription['status'] == 'PROCESSING':
                logger.info(f"Transcription already in progress for content: {content_id}")
                return {
                    'statusCode': 200,
                    'body': json.dumps({
                        'message': 'Transcription already in progress',
                        'contentId': content_id,
                        'status': existing_transcription['status']
                    })
                }
            elif existing_transcription['status'] == 'COMPLETED':
                logger.info(f"Transcription already completed for content: {content_id}")
                return {
                    'statusCode': 200,
                    'body': json.dumps({
                        'message': 'Transcription already completed',
                        'contentId': content_id,
                        'status': existing_transcription['status']
                    })
                }
        
        # Create or update transcription record
        transcription_record = create_transcription_record(content_id, s3_key, bucket_name)
        
        # Store/update in DynamoDB (upsert)
        store_transcription_record(transcription_record)
        
        # Generate unique job name (include timestamp to avoid conflicts)
        timestamp = datetime.utcnow().strftime('%Y%m%d-%H%M%S')
        job_name = f"transcription-{content_id}-{timestamp}"
        media_uri = f"s3://{bucket_name}/{s3_key}"
        
        logger.info(f"Starting transcription job: {job_name}")
        logger.info(f"Media URI: {media_uri}")
        
        transcribe_response = transcribe_client.start_transcription_job(
            TranscriptionJobName=job_name,
            Media={'MediaFileUri': media_uri},
            MediaFormat=get_audio_format(s3_key),
            LanguageCode='en-US',
            Settings={
                'ShowSpeakerLabels': False,
                'ShowAlternatives': True,
                'MaxAlternatives': 2
            },
            OutputBucketName=bucket_name,
            OutputKey=f"transcriptions/{content_id}/result.json"
        )
        
        logger.info(f"Transcribe job started: {transcribe_response['TranscriptionJob']['TranscriptionJobName']}")
        
        # Update status to PROCESSING with job name
        update_transcription_status(content_id, 'PROCESSING', {
            'jobName': job_name,
            'startedAt': datetime.utcnow().isoformat()
        })
        
        # Send message to SQS for monitoring
        send_monitoring_message(content_id, job_name)
        
        logger.info(f"Transcription started successfully: {job_name}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Transcription started successfully',
                'contentId': content_id,
                'jobName': job_name,
                'status': 'PROCESSING'
            })
        }
        
    except Exception as e:
        logger.error(f"Error starting transcription: {str(e)}")
        
        # Update status to FAILED if we have content_id
        if 'content_id' in locals():
            try:
                update_transcription_status(content_id, 'FAILED', {
                    'errorMessage': str(e),
                    'failedAt': datetime.utcnow().isoformat()
                })
            except Exception as update_error:
                logger.error(f"Error updating failed status: {str(update_error)}")
        
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Failed to start transcription'})
        }

def check_existing_transcription(content_id):
    """Check if transcription already exists for this content using contentId as key"""
    try:
        table = dynamodb.Table(os.environ['TRANSCRIPTIONS_TABLE'])
        
        # Direct get_item since contentId is now the primary key
        response = table.get_item(
            Key={'contentId': content_id}
        )
        
        return response.get('Item')
        
    except Exception as e:
        logger.error(f"Error checking existing transcription: {str(e)}")
        return None

def create_transcription_record(content_id, s3_key, bucket_name):
    """Create initial transcription record using contentId as key"""
    return {
        'contentId': content_id,  # Now the primary key
        's3Key': s3_key,
        'bucketName': bucket_name,
        'status': 'PROCESSING',
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
    """Store transcription record in DynamoDB using contentId as key"""
    table = dynamodb.Table(os.environ['TRANSCRIPTIONS_TABLE'])
    table.put_item(Item=record)
    logger.info(f"Transcription record stored for content: {record['contentId']}")

def update_transcription_status(content_id, status, additional_data=None):
    """Update transcription status using contentId as key"""
    table = dynamodb.Table(os.environ['TRANSCRIPTIONS_TABLE'])
    
    update_expression = "SET #status = :status, updatedAt = :updated_at"
    expression_values = {
        ':status': status,
        ':updated_at': datetime.utcnow().isoformat()
    }
    expression_names = {'#status': 'status'}
    
    if additional_data:
        for key, value in additional_data.items():
            # Handle reserved keywords
            if key in ['error', 'status', 'size', 'type', 'name', 'data', 'timestamp']:
                attr_name = f"#{key}"
                expression_names[attr_name] = key
                update_expression += f", {attr_name} = :{key}"
            else:
                update_expression += f", {key} = :{key}"
            expression_values[f':{key}'] = value
    
    table.update_item(
        Key={'contentId': content_id},  # Using contentId as key
        UpdateExpression=update_expression,
        ExpressionAttributeValues=expression_values,
        ExpressionAttributeNames=expression_names
    )
    
    logger.info(f"Transcription status updated: {content_id} -> {status}")

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

def send_monitoring_message(content_id, job_name):
    """Send message to SQS for transcription monitoring using contentId"""
    try:
        message = {
            'contentId': content_id,  # Using contentId instead of transcriptionId
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