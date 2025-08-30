import json
import boto3
import os
from datetime import datetime
import logging
import urllib.parse

logger = logging.getLogger()
logger.setLevel(logging.INFO)

transcribe_client = boto3.client('transcribe')
dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')
sqs = boto3.client('sqs')

def handler(event, context):
    """Monitor Transcription Handler - FIXED S3 URL parsing"""
    
    logger.info(f"Monitor transcription event: {json.dumps(event)}")
    
    processed_messages = []
    
    for record in event['Records']:
        try:
            message = json.loads(record['body'])
            transcription_id = message['transcriptionId']
            job_name = message['jobName']
            
            logger.info(f"Processing transcription: {transcription_id}, job: {job_name}")
            
            # Check transcription job status
            job_status = check_transcription_job(job_name)
            
            logger.info(f"Job {job_name} status: {job_status}")
            
            if job_status == 'COMPLETED':
                # Process completed transcription
                process_completed_transcription(transcription_id, job_name)
                processed_messages.append(record['receiptHandle'])
                
            elif job_status == 'FAILED':
                # Handle failed transcription
                handle_failed_transcription(transcription_id, job_name)
                processed_messages.append(record['receiptHandle'])
                
            elif job_status == 'IN_PROGRESS':
                # Re-queue for later monitoring (with delay)
                requeue_monitoring(transcription_id, job_name)
                processed_messages.append(record['receiptHandle'])
                
            elif job_status == 'NOT_FOUND':
                logger.error(f"Transcription job not found: {job_name}")
                # Mark as failed
                update_transcription_status(transcription_id, 'FAILED', {
                    'errorMessage': 'Transcription job not found',
                    'failedAt': datetime.utcnow().isoformat()
                })
                processed_messages.append(record['receiptHandle'])
                
            else:
                logger.warning(f"Unknown job status: {job_status}")
                processed_messages.append(record['receiptHandle'])
                
        except Exception as e:
            logger.error(f"Error processing message: {str(e)}")
            logger.error(f"Message: {record.get('body', 'No body')}")
            # Don't add to processed_messages - will retry
    
    logger.info(f"Processed {len(processed_messages)} messages")
    
    return {
        'statusCode': 200,
        'processedCount': len(processed_messages)
    }

def check_transcription_job(job_name):
    """Check the status of a transcription job"""
    try:
        response = transcribe_client.get_transcription_job(
            TranscriptionJobName=job_name
        )
        status = response['TranscriptionJob']['TranscriptionJobStatus']
        logger.info(f"Transcribe job {job_name} status: {status}")
        return status
        
    except transcribe_client.exceptions.BadRequestException as e:
        logger.error(f"Bad request for job {job_name}: {str(e)}")
        return 'NOT_FOUND'
    except Exception as e:
        logger.error(f"Error checking job {job_name}: {str(e)}")
        return 'ERROR'

def process_completed_transcription(transcription_id, job_name):
    """Process completed transcription job"""
    try:
        # Get job details
        job_response = transcribe_client.get_transcription_job(
            TranscriptionJobName=job_name
        )
        
        transcript_uri = job_response['TranscriptionJob']['Transcript']['TranscriptFileUri']
        logger.info(f"Transcript URI: {transcript_uri}")
        
        # Download and parse transcript using S3 API (not public access)
        transcript_data = download_transcript(transcript_uri)
        parsed_text = parse_transcript(transcript_data)
        
        # Update DynamoDB record
        update_transcription_completed(transcription_id, parsed_text, transcript_data)
        
        logger.info(f"Transcription completed successfully: {transcription_id}")
        
    except Exception as e:
        logger.error(f"Error processing completed transcription: {str(e)}")
        update_transcription_status(transcription_id, 'FAILED', {
            'errorMessage': str(e),
            'failedAt': datetime.utcnow().isoformat()
        })

def download_transcript(transcript_uri):
    """
    Download transcript file from S3 - FIXED for virtual hosted-style regional URLs
    Format: https://bucket.s3.region.amazonaws.com/key
    """
    try:
        logger.info(f"Parsing transcript URI: {transcript_uri}")
        
        if transcript_uri.startswith('https://'):
            # Parse HTTPS S3 URL format
            parsed_url = urllib.parse.urlparse(transcript_uri)
            logger.info(f"Parsed URL - netloc: {parsed_url.netloc}, path: {parsed_url.path}")
            
            # Handle virtual hosted-style URLs: https://bucket.s3.region.amazonaws.com/key
            if '.s3.' in parsed_url.netloc and '.amazonaws.com' in parsed_url.netloc:
                # Extract bucket from subdomain
                bucket = parsed_url.netloc.split('.s3.')[0]
                # Extract key from path
                key = parsed_url.path.lstrip('/')
                
            # Handle path-style URLs: https://s3.region.amazonaws.com/bucket/key  
            elif parsed_url.netloc.startswith('s3.') and parsed_url.netloc.endswith('.amazonaws.com'):
                path_parts = parsed_url.path.lstrip('/').split('/', 1)
                bucket = path_parts[0]
                key = path_parts[1] if len(path_parts) > 1 else ''
                
            # Handle legacy format: https://s3.amazonaws.com/bucket/key
            elif parsed_url.netloc == 's3.amazonaws.com':
                path_parts = parsed_url.path.lstrip('/').split('/', 1)
                bucket = path_parts[0]
                key = path_parts[1] if len(path_parts) > 1 else ''
                
            else:
                raise ValueError(f"Unknown S3 URL format: {transcript_uri}")
                
        elif transcript_uri.startswith('s3://'):
            # Parse S3 URI format: s3://bucket/key
            uri_parts = transcript_uri.replace('s3://', '').split('/', 1)
            bucket = uri_parts[0]
            key = uri_parts[1] if len(uri_parts) > 1 else ''
        else:
            raise ValueError(f"Unsupported URI format: {transcript_uri}")
        
        logger.info(f"Extracted bucket: '{bucket}', key: '{key}'")
        
        if not bucket or not key:
            raise ValueError(f"Could not extract bucket/key from URI: {transcript_uri}")
        
        # Download from S3 using AWS credentials (not public access)
        logger.info(f"Downloading from S3: s3://{bucket}/{key}")
        response = s3.get_object(Bucket=bucket, Key=key)
        transcript_data = json.loads(response['Body'].read())
        
        logger.info(f"Transcript downloaded successfully from s3://{bucket}/{key}")
        return transcript_data
        
    except Exception as e:
        logger.error(f"Error downloading transcript from {transcript_uri}: {str(e)}")
        raise

def parse_transcript(transcript_data):
    """Parse transcript data to extract text"""
    try:
        transcripts = transcript_data.get('results', {}).get('transcripts', [])
        if not transcripts:
            logger.warning("No transcripts found in transcript data")
            return {
                'text': '',
                'confidence': 0,
                'word_count': 0,
                'items': []
            }
        
        text = transcripts[0].get('transcript', '')
        
        items = transcript_data.get('results', {}).get('items', [])
        confidence_scores = []
        
        for item in items:
            alternatives = item.get('alternatives', [])
            if alternatives:
                confidence = alternatives[0].get('confidence')
                if confidence is not None:
                    confidence_scores.append(str(confidence))
        
        avg_confidence = sum(confidence_scores) / len(confidence_scores) if confidence_scores else 0
        
        result = {
            'text': text,
            'confidence': avg_confidence,
            'word_count': len(text.split()) if text else 0,
            'items': items
        }
        
        logger.info(f"Transcript parsed: {len(text)} chars, confidence: {avg_confidence:.2%}, words: {result['word_count']}")
        return result
        
    except Exception as e:
        logger.error(f"Error parsing transcript: {str(e)}")
        return {
            'text': '',
            'confidence': 0,
            'word_count': 0,
            'items': []
        }

def update_transcription_completed(transcription_id, parsed_text, raw_data):
    """Update transcription record with completed data"""
    try:
        table = dynamodb.Table(os.environ['TRANSCRIPTIONS_TABLE'])
        
        table.update_item(
            Key={'transcriptionId': transcription_id},
            UpdateExpression="""
                SET #status = :status,
                    transcriptionText = :text,
                    confidence = :confidence,
                    wordCount = :word_count,
                    completedAt = :completed_at,
                    updatedAt = :updated_at,
                    rawData = :raw_data
            """,
            ExpressionAttributeNames={
                '#status': 'status'
            },
            ExpressionAttributeValues={
                ':status': 'COMPLETED',
                ':text': parsed_text['text'],
                ':confidence': parsed_text['confidence'],
                ':word_count': parsed_text['word_count'],
                ':completed_at': datetime.utcnow().isoformat(),
                ':updated_at': datetime.utcnow().isoformat(),
                ':raw_data': raw_data
            }
        )
        
        logger.info(f"Transcription marked as completed: {transcription_id}")
        
    except Exception as e:
        logger.error(f"Error updating completed transcription: {str(e)}")
        raise

def update_transcription_status(transcription_id, status, additional_data=None):
    """Update transcription status"""
    try:
        table = dynamodb.Table(os.environ['TRANSCRIPTIONS_TABLE'])
        
        update_expression = "SET #status = :status, updatedAt = :updated_at"
        expression_values = {
            ':status': status,
            ':updated_at': datetime.utcnow().isoformat()
        }
        expression_names = {
            '#status': 'status'
        }
        
        if additional_data:
            for key, value in additional_data.items():
                if key in ['error', 'status', 'size', 'type', 'name', 'data', 'timestamp']:
                    attr_name = f"#{key}"
                    expression_names[attr_name] = key
                    update_expression += f", {attr_name} = :{key}"
                else:
                    update_expression += f", {key} = :{key}"
                expression_values[f':{key}'] = value
        
        table.update_item(
            Key={'transcriptionId': transcription_id},
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_values,
            ExpressionAttributeNames=expression_names
        )
        
        logger.info(f"Transcription status updated: {transcription_id} -> {status}")
        
    except Exception as e:
        logger.error(f"Error updating transcription status: {str(e)}")
        raise

def handle_failed_transcription(transcription_id, job_name):
    """Handle failed transcription with retry logic"""
    try:
        table = dynamodb.Table(os.environ['TRANSCRIPTIONS_TABLE'])
        response = table.get_item(Key={'transcriptionId': transcription_id})
        
        if 'Item' not in response:
            logger.error(f"Transcription record not found: {transcription_id}")
            return
            
        record = response['Item']
        retry_count = record.get('retryCount', 0)
        max_retries = 3
        
        logger.info(f"Handling failed transcription: {transcription_id}, retries: {retry_count}/{max_retries}")
        
        if retry_count < max_retries:
            retry_transcription(transcription_id, record)
        else:
            update_transcription_status(transcription_id, 'FAILED', {
                'errorMessage': 'Max retries exceeded',
                'failedAt': datetime.utcnow().isoformat()
            })
            logger.info(f"Max retries exceeded for transcription: {transcription_id}")
            
    except Exception as e:
        logger.error(f"Error handling failed transcription: {str(e)}")

def retry_transcription(transcription_id, record):
    """Retry failed transcription"""
    try:
        table = dynamodb.Table(os.environ['TRANSCRIPTIONS_TABLE'])
        table.update_item(
            Key={'transcriptionId': transcription_id},
            UpdateExpression="SET retryCount = retryCount + :one, updatedAt = :updated_at",
            ExpressionAttributeValues={
                ':one': 1,
                ':updated_at': datetime.utcnow().isoformat()
            }
        )
        
        retry_job_name = f"transcription-retry-{transcription_id}-{record.get('retryCount', 0) + 1}"
        media_uri = f"s3://{record['bucketName']}/{record['s3Key']}"
        
        transcribe_client.start_transcription_job(
            TranscriptionJobName=retry_job_name,
            Media={'MediaFileUri': media_uri},
            MediaFormat=get_audio_format(record['s3Key']),
            LanguageCode='en-US',
            Settings={
                'ShowSpeakerLabels': False,
                'MaxSpeakerLabels': 1
            }
        )
        
        send_monitoring_message(transcription_id, retry_job_name)
        logger.info(f"Transcription retry started: {retry_job_name}")
        
    except Exception as e:
        logger.error(f"Error retrying transcription: {str(e)}")

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

def requeue_monitoring(transcription_id, job_name):
    """Re-queue transcription for monitoring"""
    try:
        message = {
            'transcriptionId': transcription_id,
            'jobName': job_name,
            'action': 'monitor',
            'timestamp': datetime.utcnow().isoformat()
        }
        
        sqs.send_message(
            QueueUrl=os.environ['TRANSCRIPTION_QUEUE_URL'],
            MessageBody=json.dumps(message),
            DelaySeconds=120
        )
        
        logger.info(f"Transcription re-queued for monitoring: {transcription_id}")
        
    except Exception as e:
        logger.error(f"Error re-queuing monitoring: {str(e)}")

def send_monitoring_message(transcription_id, job_name):
    """Send message to SQS for transcription monitoring"""
    try:
        message = {
            'transcriptionId': transcription_id,
            'jobName': job_name,
            'action': 'monitor',
            'timestamp': datetime.utcnow().isoformat()
        }
        
        sqs.send_message(
            QueueUrl=os.environ['TRANSCRIPTION_QUEUE_URL'],
            MessageBody=json.dumps(message),
            DelaySeconds=60
        )
        
        logger.info(f"Monitoring message sent: {transcription_id}")
        
    except Exception as e:
        logger.error(f"Error sending monitoring message: {str(e)}")