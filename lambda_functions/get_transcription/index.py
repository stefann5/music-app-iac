import json
import boto3
import os
from datetime import datetime
import logging
from decimal import Decimal

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')

def handler(event, context):
    """
    Get Transcription Handler - Enhanced with HTML formatting for subtitles
    Returns transcription with timing data for frontend subtitle display
    """
    
    try:
        # Get contentId from query parameters
        query_params = event.get('queryStringParameters') or {}
        content_id = query_params.get('contentId')
        format_type = query_params.get('format', 'json')  # 'json', 'html', 'srt', 'vtt'
        
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
        
        # Base response data
        response_data = {
            'contentId': content_id,
            'transcriptionId': transcription['transcriptionId'],
            'status': transcription['status'],
            'createdAt': transcription['createdAt'],
            'updatedAt': transcription['updatedAt']
        }
        
        if transcription['status'] == 'COMPLETED':
            # Parse raw data for enhanced formatting
            raw_data = transcription.get('rawData', {})
            
            # Add basic text info
            response_data.update({
                'text': transcription['transcriptionText'],
                'confidence': float(transcription.get('confidence', 0)) if isinstance(transcription.get('confidence'), Decimal) else transcription.get('confidence', 0),
                'wordCount': int(transcription.get('wordCount', 0)) if isinstance(transcription.get('wordCount'), Decimal) else transcription.get('wordCount', 0),
                'completedAt': transcription.get('completedAt')
            })
            
            # Add enhanced formatting based on request
            if format_type == 'html':
                response_data['html'] = generate_html_subtitles(raw_data)
                response_data['css'] = get_subtitle_css()
            elif format_type == 'srt':
                response_data['srt'] = generate_srt_subtitles(raw_data)
            elif format_type == 'vtt':
                response_data['vtt'] = generate_vtt_subtitles(raw_data)
            elif format_type == 'json':
                response_data['words'] = extract_word_timing(raw_data)
                response_data['segments'] = extract_segments(raw_data)
            
        elif transcription['status'] == 'PROCESSING':
            response_data['message'] = 'Transcription is still being processed'
        elif transcription['status'] == 'FAILED':
            response_data['message'] = 'Transcription failed'
            response_data['errorMessage'] = transcription.get('errorMessage', 'Unknown error')
            
        return create_success_response(200, response_data)
        
    except Exception as e:
        logger.error(f"Error getting transcription: {str(e)}")
        return create_error_response(500, "Internal server error")

def generate_html_subtitles(raw_data):
    """
    Generate HTML with timing data for synchronized subtitles
    Creates spans with data attributes for timing
    """
    try:
        results = raw_data.get('results', {})
        items = results.get('items', [])
        
        if not items:
            return '<div class="transcription-empty">No transcription data available</div>'
        
        html_parts = []
        html_parts.append('<div class="transcription-container" data-type="subtitles">')
        
        # Group words into sentences/phrases for better subtitle display
        current_sentence = []
        sentence_start = None
        sentence_end = None
        
        for i, item in enumerate(items):
            if item.get('type') != 'pronunciation':
                continue
                
            alternatives = item.get('alternatives', [])
            if not alternatives:
                continue
                
            word_data = alternatives[0]
            content = word_data.get('content', '')
            confidence = float(word_data.get('confidence', 0))
            start_time = float(item.get('start_time', 0))
            end_time = float(item.get('end_time', 0))
            
            # Start new sentence
            if sentence_start is None:
                sentence_start = start_time
            
            sentence_end = end_time
            current_sentence.append({
                'content': content,
                'confidence': confidence,
                'start_time': start_time,
                'end_time': end_time
            })
            
            # End sentence on punctuation or after 5 words or significant time gap
            is_sentence_end = (
                content.endswith(('.', '!', '?')) or 
                len(current_sentence) >= 5 or
                (i + 1 < len(items) and 
                 float(items[i + 1].get('start_time', 0)) - end_time > 2.0)
            )
            
            if is_sentence_end or i == len(items) - 1:
                # Create subtitle segment
                segment_html = create_subtitle_segment(current_sentence, sentence_start, sentence_end, len(html_parts) - 1)
                html_parts.append(segment_html)
                
                # Reset for next sentence
                current_sentence = []
                sentence_start = None
                sentence_end = None
        
        html_parts.append('</div>')
        return '\n'.join(html_parts)
        
    except Exception as e:
        logger.error(f"Error generating HTML subtitles: {str(e)}")
        return f'<div class="transcription-error">Error generating subtitles: {str(e)}</div>'

def create_subtitle_segment(words, start_time, end_time, index):
    """Create HTML for a subtitle segment with timing data"""
    
    # Calculate average confidence
    confidences = [w['confidence'] for w in words if w['confidence'] > 0]
    avg_confidence = sum(confidences) / len(confidences) if confidences else 0
    
    # Determine confidence class for styling
    if avg_confidence >= 0.8:
        confidence_class = 'high-confidence'
    elif avg_confidence >= 0.5:
        confidence_class = 'medium-confidence'
    else:
        confidence_class = 'low-confidence'
    
    # Create text with individual word spans
    word_spans = []
    for word in words:
        word_span = f'''<span class="transcription-word" 
                           data-start="{word['start_time']:.3f}" 
                           data-end="{word['end_time']:.3f}" 
                           data-confidence="{word['confidence']:.3f}">
                           {word['content']}
                       </span>'''
        word_spans.append(word_span)
    
    text_content = ' '.join(word_spans)
    
    # Create subtitle segment
    segment_html = f'''
    <div class="transcription-segment {confidence_class}" 
         data-index="{index}"
         data-start="{start_time:.3f}" 
         data-end="{end_time:.3f}" 
         data-duration="{end_time - start_time:.3f}"
         data-confidence="{avg_confidence:.3f}">
        <div class="segment-text">
            {text_content}
        </div>
        <div class="segment-timing">
            {format_time(start_time)} - {format_time(end_time)}
        </div>
    </div>'''
    
    return segment_html

def generate_srt_subtitles(raw_data):
    """Generate SRT format subtitles"""
    try:
        results = raw_data.get('results', {})
        segments = results.get('segments', [])
        
        if not segments:
            # Fallback to items if no segments
            items = results.get('items', [])
            return generate_srt_from_items(items)
        
        srt_lines = []
        
        for i, segment in enumerate(segments, 1):
            start_time = float(segment.get('start_time', 0))
            end_time = float(segment.get('end_time', 0))
            
            # Get best alternative transcript
            alternatives = segment.get('alternatives', [])
            if alternatives:
                transcript = alternatives[0].get('transcript', '')
            else:
                transcript = ''
            
            if transcript.strip():
                srt_lines.append(f"{i}")
                srt_lines.append(f"{format_srt_time(start_time)} --> {format_srt_time(end_time)}")
                srt_lines.append(transcript.strip())
                srt_lines.append("")  # Empty line between subtitles
        
        return '\n'.join(srt_lines)
        
    except Exception as e:
        logger.error(f"Error generating SRT: {str(e)}")
        return f"Error generating SRT: {str(e)}"

def generate_vtt_subtitles(raw_data):
    """Generate WebVTT format subtitles"""
    try:
        vtt_lines = ["WEBVTT", ""]  # WebVTT header
        
        results = raw_data.get('results', {})
        segments = results.get('segments', [])
        
        if not segments:
            items = results.get('items', [])
            return generate_vtt_from_items(items)
        
        for i, segment in enumerate(segments):
            start_time = float(segment.get('start_time', 0))
            end_time = float(segment.get('end_time', 0))
            
            alternatives = segment.get('alternatives', [])
            if alternatives:
                transcript = alternatives[0].get('transcript', '')
            else:
                transcript = ''
            
            if transcript.strip():
                vtt_lines.append(f"{format_vtt_time(start_time)} --> {format_vtt_time(end_time)}")
                vtt_lines.append(transcript.strip())
                vtt_lines.append("")
        
        return '\n'.join(vtt_lines)
        
    except Exception as e:
        logger.error(f"Error generating VTT: {str(e)}")
        return f"Error generating VTT: {str(e)}"

def extract_word_timing(raw_data):
    """Extract individual word timing data for frontend use"""
    try:
        results = raw_data.get('results', {})
        items = results.get('items', [])
        
        words = []
        for item in items:
            if item.get('type') != 'pronunciation':
                continue
                
            alternatives = item.get('alternatives', [])
            if not alternatives:
                continue
                
            word_data = alternatives[0]
            words.append({
                'content': word_data.get('content', ''),
                'confidence': float(word_data.get('confidence', 0)),
                'startTime': float(item.get('start_time', 0)),
                'endTime': float(item.get('end_time', 0)),
                'duration': float(item.get('end_time', 0)) - float(item.get('start_time', 0))
            })
        
        return words
        
    except Exception as e:
        logger.error(f"Error extracting word timing: {str(e)}")
        return []

def extract_segments(raw_data):
    """Extract segment timing data"""
    try:
        results = raw_data.get('results', {})
        segments = results.get('segments', [])
        
        segment_data = []
        for segment in segments:
            alternatives = segment.get('alternatives', [])
            if alternatives:
                segment_data.append({
                    'transcript': alternatives[0].get('transcript', ''),
                    'startTime': float(segment.get('start_time', 0)),
                    'endTime': float(segment.get('end_time', 0)),
                    'alternatives': alternatives
                })
        
        return segment_data
        
    except Exception as e:
        logger.error(f"Error extracting segments: {str(e)}")
        return []

def get_subtitle_css():
    """Return CSS for styling subtitles"""
    return """
    .transcription-container {
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
        max-width: 800px;
        margin: 0 auto;
        padding: 20px;
        background: #f8f9fa;
        border-radius: 8px;
    }
    
    .transcription-segment {
        margin-bottom: 15px;
        padding: 12px;
        background: white;
        border-radius: 6px;
        border-left: 4px solid #007bff;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        transition: all 0.3s ease;
    }
    
    .transcription-segment:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 8px rgba(0,0,0,0.15);
    }
    
    .transcription-segment.high-confidence {
        border-left-color: #28a745;
    }
    
    .transcription-segment.medium-confidence {
        border-left-color: #ffc107;
    }
    
    .transcription-segment.low-confidence {
        border-left-color: #dc3545;
    }
    
    .segment-text {
        font-size: 16px;
        line-height: 1.5;
        margin-bottom: 8px;
        color: #333;
    }
    
    .transcription-word {
        display: inline;
        padding: 2px 4px;
        border-radius: 3px;
        transition: background-color 0.2s ease;
        cursor: pointer;
    }
    
    .transcription-word:hover {
        background-color: #e3f2fd;
    }
    
    .transcription-word[data-confidence^="0.9"] {
        background-color: rgba(40, 167, 69, 0.1);
    }
    
    .transcription-word[data-confidence^="0.8"] {
        background-color: rgba(40, 167, 69, 0.05);
    }
    
    .transcription-word[data-confidence^="0.5"], 
    .transcription-word[data-confidence^="0.6"], 
    .transcription-word[data-confidence^="0.7"] {
        background-color: rgba(255, 193, 7, 0.1);
    }
    
    .transcription-word[data-confidence^="0.1"], 
    .transcription-word[data-confidence^="0.2"], 
    .transcription-word[data-confidence^="0.3"], 
    .transcription-word[data-confidence^="0.4"] {
        background-color: rgba(220, 53, 69, 0.1);
    }
    
    .segment-timing {
        font-size: 12px;
        color: #666;
        font-family: monospace;
        text-align: right;
    }
    
    .transcription-empty, .transcription-error {
        text-align: center;
        padding: 40px;
        color: #666;
        font-style: italic;
    }
    
    .transcription-error {
        background-color: #fff3cd;
        border: 1px solid #ffeeba;
        color: #856404;
    }
    
    /* Active/playing subtitle styling */
    .transcription-segment.active {
        background-color: #007bff;
        color: white;
        transform: scale(1.02);
    }
    
    .transcription-segment.active .segment-timing {
        color: rgba(255, 255, 255, 0.8);
    }
    """

def format_time(seconds):
    """Format seconds to MM:SS.mmm format"""
    minutes = int(seconds // 60)
    seconds = seconds % 60
    return f"{minutes:02d}:{seconds:06.3f}"

def format_srt_time(seconds):
    """Format seconds to SRT time format (HH:MM:SS,mmm)"""
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = seconds % 60
    return f"{hours:02d}:{minutes:02d}:{secs:06.3f}".replace('.', ',')

def format_vtt_time(seconds):
    """Format seconds to VTT time format (HH:MM:SS.mmm)"""
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = seconds % 60
    return f"{hours:02d}:{minutes:02d}:{secs:06.3f}"

def generate_srt_from_items(items):
    """Generate SRT from individual items when no segments available"""
    srt_lines = []
    subtitle_index = 1
    
    # Group words into subtitle chunks
    current_chunk = []
    chunk_start = None
    
    for item in items:
        if item.get('type') != 'pronunciation':
            continue
            
        alternatives = item.get('alternatives', [])
        if not alternatives:
            continue
            
        word = alternatives[0].get('content', '')
        start_time = float(item.get('start_time', 0))
        end_time = float(item.get('end_time', 0))
        
        if chunk_start is None:
            chunk_start = start_time
            
        current_chunk.append(word)
        
        # End chunk after 5 words or 3 seconds
        if len(current_chunk) >= 5 or (end_time - chunk_start) >= 3.0:
            text = ' '.join(current_chunk)
            srt_lines.append(f"{subtitle_index}")
            srt_lines.append(f"{format_srt_time(chunk_start)} --> {format_srt_time(end_time)}")
            srt_lines.append(text)
            srt_lines.append("")
            
            subtitle_index += 1
            current_chunk = []
            chunk_start = None
    
    # Handle remaining words
    if current_chunk and chunk_start is not None:
        text = ' '.join(current_chunk)
        srt_lines.append(f"{subtitle_index}")
        srt_lines.append(f"{format_srt_time(chunk_start)} --> {format_srt_time(end_time)}")
        srt_lines.append(text)
    
    return '\n'.join(srt_lines)

def generate_vtt_from_items(items):
    """Generate VTT from individual items when no segments available"""
    vtt_lines = ["WEBVTT", ""]
    
    # Similar logic to SRT but with VTT formatting
    current_chunk = []
    chunk_start = None
    
    for item in items:
        if item.get('type') != 'pronunciation':
            continue
            
        alternatives = item.get('alternatives', [])
        if not alternatives:
            continue
            
        word = alternatives[0].get('content', '')
        start_time = float(item.get('start_time', 0))
        end_time = float(item.get('end_time', 0))
        
        if chunk_start is None:
            chunk_start = start_time
            
        current_chunk.append(word)
        
        if len(current_chunk) >= 5 or (end_time - chunk_start) >= 3.0:
            text = ' '.join(current_chunk)
            vtt_lines.append(f"{format_vtt_time(chunk_start)} --> {format_vtt_time(end_time)}")
            vtt_lines.append(text)
            vtt_lines.append("")
            
            current_chunk = []
            chunk_start = None
    
    if current_chunk and chunk_start is not None:
        text = ' '.join(current_chunk)
        vtt_lines.append(f"{format_vtt_time(chunk_start)} --> {format_vtt_time(end_time)}")
        vtt_lines.append(text)
    
    return '\n'.join(vtt_lines)

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