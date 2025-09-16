from collections import defaultdict
from datetime import datetime, timedelta
import json
import boto3
import os
from typing import Counter, Dict, Any
import decimal
import logging

dynamodb = boto3.resource('dynamodb')
s3_client = boto3.client('s3')
lambda_client = boto3.client('lambda')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event, context):
    try:
        request_context = event.get('requestContext', {})
        authorizer = request_context.get('authorizer', {})
        username = authorizer.get('username', {})

        table_name = os.environ['MUSIC_CONTENT_TABLE']
        bucket_name = os.environ['MUSIC_CONTENT_BUCKET']
        table = dynamodb.Table(table_name)

        subscriptions = get_subscriptions(username)
        ratings = get_ratings(username)
        history = get_user_history(username)


        albums = get_all_albums()
        
        content  = _get_all_content(table)
        
        feed_albums = get_feed_albums(subscriptions, ratings, history, albums, content)
        
        store_feed(username, feed_albums)

        return {
            'statusCode': 200,
            'headers': get_cors_headers(),
            'body': json.dumps('OK')
        }

        
    except Exception as e:
        print(f"Error processing request: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

def get_feed_albums(subscriptions, ratings, history, albums, content):
    """
    Generiše personalizovani feed albuma na osnovu korisničkih podataka
    
    Args:
        subscriptions: lista pretplata {type: "ARTIST"/"GENRE", targetName: string, artistId?: string}
        ratings: lista ocena {songId: string, stars: int}
        history: lista istorije {genre: string, artist: string, timestamp: datetime}
        albums: lista svih albuma {id, artistId, artistName, genre, songs: [...]}
    
    Returns:
        Lista albuma sortiranih po afinitetu (opadajuće)
    """
    
    subscription_boost = {}

    for sub in subscriptions:
        if sub['subscriptionType'] == 'ARTIST':
            for album in albums:
                if album['artistId'] == sub.get('artistId'):
                    subscription_boost[album['albumId']] = subscription_boost.get(album['albumId'], 0) + 50
                    
        elif sub['subscriptionType'] == 'GENRE':
            for album in albums:
                if album['genre'].lower() == sub['targetName'].lower():
                    subscription_boost[album['albumId']] = subscription_boost.get(album['albumId'], 0) + 30
    
    song_ratings = {rating['songId']: int(rating['stars']) for rating in ratings}


    album_ratings = defaultdict(list)
    genre_affinity = defaultdict(list)
    artist_affinity = defaultdict(list)
    
    for album in albums:
        album_song_ratings = []
        for song in [item for item in content if item.get("albumId") == album['albumId']]:
            if song.get('contentId') in song_ratings:
                rating = song_ratings[song['contentId']]
                album_song_ratings.append(rating)
                genre_affinity[album['genre']].append(rating)
                artist_affinity[album['artistId']].append(rating)
    
    if album_song_ratings:
        album_ratings[album['albumId']] = sum(album_song_ratings) / len(album_song_ratings)
    

    avg_genre_ratings = {}
    for genre, ratings_list in genre_affinity.items():
        avg_genre_ratings[genre] = sum(ratings_list) / len(ratings_list)
    
    avg_artist_ratings = {}
    for artist_id, ratings_list in artist_affinity.items():
        avg_artist_ratings[artist_id] = sum(ratings_list) / len(ratings_list)
    
    

    now = datetime.now()
    recent_threshold = now - timedelta(days=30)  # Skorašnja istorija
    
    genre_frequency = Counter()
    artist_frequency = Counter()
    recent_genre_frequency = Counter()
    recent_artist_frequency = Counter()
    
    hourly_genre_preferences = defaultdict(Counter)
    current_hour = now.hour
    
    for entry in history:
        timestamp = datetime.fromisoformat(entry['timestamp'])
        genre = entry['genre']
        artist = entry['artist']
        
        genre_frequency[genre] += 1
        artist_frequency[artist] += 1
        
        if timestamp >= recent_threshold:
            recent_genre_frequency[genre] += 2
            recent_artist_frequency[artist] += 2
        
        
        hour = timestamp.hour
        hourly_genre_preferences[hour][genre] += 1
    
    
    album_scores = {}
    
    
    for album in albums:
        
        score = 0
        album_id = album['albumId']
        genre = album['genre']
        artist_id = album['artistId']
        
        score += subscription_boost.get(album_id, 0)
        
        if album_id in album_ratings:
            album_rating = album_ratings[album_id]
            if album_rating >= 4:
                score += (album_rating - 3) * 20  
            elif album_rating <= 2:
                score -= (3 - album_rating) * 15  

        if genre in avg_genre_ratings:
            genre_rating = avg_genre_ratings[genre]
            if genre_rating >= 3.5:
                score += (genre_rating - 3) * 15
            elif genre_rating <= 2.5:
                score -= (3 - genre_rating) * 10
        

        if artist_id in avg_artist_ratings:
            artist_rating = avg_artist_ratings[artist_id]
            if artist_rating >= 3.5:
                score += (artist_rating - 3) * 25
            elif artist_rating <= 2.5:
                score -= (3 - artist_rating) * 15
        
        total_genre_plays = genre_frequency.get(genre, 0) + recent_genre_frequency.get(genre, 0)
        score += min(total_genre_plays * 2, 30)  
        
        total_artist_plays = artist_frequency.get(artist_id, 0) + recent_artist_frequency.get(artist_id, 0)
        score += min(total_artist_plays * 3, 40)  
        
        if genre in hourly_genre_preferences.get(current_hour, {}):
            time_preference = hourly_genre_preferences[current_hour][genre]
            score += min(time_preference * 5, 25) 
        
        if recent_genre_frequency[genre] > genre_frequency[genre] * 0.3:
            score += 15  
        
        if genre in avg_genre_ratings and avg_genre_ratings[genre] < 2:
            score -= 20
        
        album['stats']['score'] = score

        album_scores[album_id] = score  
    
    
    sorted_albums = sorted(albums, 
                          key=lambda album: album_scores.get(album['albumId'], 0), 
                          reverse=True)
    
    sorted_albums = convert_decimals_to_float(sorted_albums)

    return sorted_albums


def store_feed(username, feed):
    """Update user's feed with given album list"""
    try:
        table = dynamodb.Table(os.environ['FEED_TABLE'])  # zameni sa svojom tabelom

        # prvo proveravamo da li postoji korisnik
        response = table.get_item(
            Key={'username': username}
        )

        print(response)

        if 'Item' not in response:
            logger.warning(f"User not found: {username}")
            raise ValueError(f"User {username} does not exist!")

        feed = convert_floats_to_decimal(feed)

        # updejtujemo feed kolonu
        table.update_item(
            Key={'username': username},
            UpdateExpression="SET #feed = :feed",
            ExpressionAttributeNames={
                '#feed': 'feed'
            },
            ExpressionAttributeValues={
                ':feed': feed
            }
        )

        logger.info(f"Feed updated successfully for {username}: {feed}")
        return {"statusCode": 200, "message": "Feed updated successfully"}

    except ValueError as e:
        return {"statusCode": 404, "error": str(e)}
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise

def convert_decimals_to_float(obj):
    """Rekurzivno konvertuje sve Decimal objekte u float"""
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    elif isinstance(obj, dict):
        return {key: convert_decimals_to_float(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_decimals_to_float(item) for item in obj]
    else:
        return obj
    
def convert_floats_to_decimal(obj):
    """Rekurzivno konvertuje sve Decimal objekte u float"""
    if isinstance(obj, float):
        return decimal.Decimal(obj)
    elif isinstance(obj, dict):
        return {key: convert_floats_to_decimal(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_floats_to_decimal(item) for item in obj]
    else:
        return obj

def _get_all_content(table):
    try:

        scan_kwargs = {
            
        }

        response = table.scan(**scan_kwargs)
        items = [_sanitize_item(item) for item in response.get('Items', [])]

        return items
    except Exception as e:
        print(f"Error fetching all content: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Failed to get all content'})
        }

def get_all_albums():
    """Get all albums with pagination"""
    try:
        
        table = dynamodb.Table(os.environ['ALBUMS_TABLE'])
        
        scan_params = {
            'FilterExpression': '#status = :status',
            'ExpressionAttributeNames': {'#status': 'status'},
            'ExpressionAttributeValues': {':status': 'active'}
        }
        
        
        response = table.scan(**scan_params)
        
        albums = []
        for item in response.get('Items', []):
            album = transform_album_for_response(item)
            albums.append(album)
        
        logger.info(f"Retrieved {len(albums)} albums")
        
        return albums
        
    except Exception as e:
        logger.error(f"Error getting all albums: {str(e)}")
        raise


def transform_album_for_response(item):
    """Transform DynamoDB album item to frontend-friendly format"""
    # Convert Decimal to float for JSON serialization
    for key, value in item.items():
        if isinstance(value, decimal.Decimal):
            item[key] = float(value)
    
    return {
        'albumId': item.get('albumId'),
        'title': item.get('title'),
        'artistId': item.get('artistId'),
        'genre': item.get('genre'),
        'description': item.get('description', ''),
        'releaseYear': item.get('releaseYear'),
        'trackCount': item.get('trackCount', 0),
        'duration': item.get('duration', 0),
        'coverImageUrl': item.get('coverImageUrl', ''),
        'status': item.get('status'),
        'createdAt': item.get('createdAt'),
        'updatedAt': item.get('updatedAt'),
        'metadata': item.get('metadata', {}),
        'recordLabel': item.get('recordLabel', ''),
        'producer': item.get('producer', ''),
        'tags': item.get('tags', []),
        'isExplicit': item.get('isExplicit', False),
        'stats': {
            'score': 0
        }   
    }

#Removes sensitive fields from DynamoDB item before returning to client
def _sanitize_item(item):
    safe_item = dict(item)
    #Remove sensitive fields
    safe_fields_to_remove = ['bucketName', 's3Key', 'coverImageS3Key']
    for field in safe_fields_to_remove:
        safe_item.pop(field, None)
    #Convert Decimal types to float for JSON serialization
    for key, value in safe_item.items():
        if isinstance(value, decimal.Decimal):
            safe_item[key] = float(value)
        else:
            safe_item[key] = value
    
    return safe_item


def get_subscriptions(username):
    """Get subscriptions from DynamoDB with optional pagination and filtering"""
    try:
        # Proveri da li postoji environment varijabla
        table_name = os.environ.get('SUBSCRIPTIONS_TABLE')
        if not table_name:
            logger.error("SUBSCRIPTIONS_TABLE environment variable is not set")
            raise ValueError("SUBSCRIPTIONS_TABLE environment variable is not set")
        
        table = dynamodb.Table(table_name)
        
        # Scan parameters - ispravio sam i FilterExpression
        scan_params = {
            'FilterExpression': '#username = :username',  # Tačno poklapanje umesto contains
            'ExpressionAttributeNames': {
                '#username': 'username'
            },
            'ExpressionAttributeValues': {
                ':username': username
            }
        }
        
        # Perform scan
        response = table.scan(**scan_params)
        
        # Transform subscriptions data for frontend
        subscriptions = []
        for item in response.get('Items', []):
            subscription = transform_subscription_for_response(item)
            subscriptions.append(subscription)
        
        result = subscriptions  # Ispravio sam - treba da bude subscriptions, ne subscription
        
        return result
        
    except Exception as e:
        logger.error(f"Error getting subscriptions: {str(e)}")
        raise

def transform_subscription_for_response(item):
    """Transform DynamoDB item to frontend-friendly format"""
    return {
        'subscriptionId': item.get('subscriptionId'),
        'username': item.get('username'), 
        'subscriptionType': item.get('subscriptionType'), 
        'targetId': item.get('targetId'),
        'targetName': item.get('targetName'),
        'timestamp': item.get('timestamp')
    }

def get_ratings(username):
    """Get ratings from DynamoDB with optional pagination and filtering"""
    try:
        table = dynamodb.Table(os.environ['RATINGS_TABLE'])
        
        # Scan parameters
        scan_params = {
        }

        scan_params['FilterExpression'] = 'contains(username, :username)'
        scan_params['ExpressionAttributeValues'] = {':username': username}
        
        # Perform scan
        response = table.scan(**scan_params)
        
        # Transform artists data for frontend
        ratings = []
        for item in response.get('Items', []):
            rating = transform_rating_for_response(item)
            ratings.append(rating)
        
        result = ratings
        
        return result
        
    except Exception as e:
        logger.error(f"Error getting ratings: {str(e)}")
        raise

def transform_rating_for_response(item):
    """Transform DynamoDB item to frontend-friendly format"""
    return {
        'ratingId': item.get('ratingId'),
        'username': item.get('username'),
        'songId': item.get('songId'),
        'stars': item.get('stars'),
        'timestamp': item.get('timestamp')
    }


def get_user_history(username):
    """Get user listening history from DynamoDB users table"""
    try:
        table = dynamodb.Table(os.environ['USERS_TABLE'])
        
        # Scan parameters to find user by username
        scan_params = {
            'FilterExpression': 'username = :username',
            'ExpressionAttributeValues': {':username': username}
        }
        
        # Perform scan
        response = table.scan(**scan_params)
        
        # Check if user exists
        items = response.get('Items', [])
        if not items:
            logger.warning(f"User not found: {username}")
            return []
        
        user = items[0]  # Should be only one user with this username
        
        # Extract listening history from stats
        listening_history = user.get('stats', {}).get('llisteningHistory', [])
        
        return listening_history
        
    except Exception as e:
        logger.error(f"Error getting user history for {username}: {str(e)}")
        raise

def get_cors_headers():
    """Get CORS headers for API responses"""
    return {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type,Authorization,X-Requested-With',
        'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',
        'Content-Type': 'application/json'
    }