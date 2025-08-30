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
        
        result = {
            'content': feed_albums,
            'count': len(feed_albums)
        }
        
        return {
            'statusCode': 200,
            'headers': get_cors_headers(),
            'body': json.dumps(result)
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
    
    # Korak 1: Analiza pretplata - direktan uticaj
    subscription_boost = {}

    for sub in subscriptions:
        if sub['subscriptionType'] == 'ARTIST':
            # Pretplata na artiste - snažan pozitivan signal
            for album in albums:
                if album['artistId'] == sub.get('artistId'):
                    subscription_boost[album['albumId']] = subscription_boost.get(album['albumId'], 0) + 50
                    
        elif sub['subscriptionType'] == 'GENRE':
            # Pretplata na žanrove - umeren pozitivan signal
            for album in albums:
                if album['genre'].lower() == sub['targetName'].lower():
                    subscription_boost[album['albumId']] = subscription_boost.get(album['albumId'], 0) + 30
    
    # Korak 2: Analiza ocena - afinitet prema žanrovima i artistima
    song_ratings = {rating['songId']: int(rating['stars']) for rating in ratings}

    # Mapiranje pesama na albume i računanje prosečnih ocena
    album_ratings = defaultdict(list)
    genre_affinity = defaultdict(list)
    artist_affinity = defaultdict(list)
    
    for album in albums:
        album_song_ratings = []
        # Prolazimo kroz sve pesme ovog albuma
        for song in content.get(album['albumId'], []):
            if song.get('contentId') in song_ratings:
                rating = song_ratings[song['contentId']]
                album_song_ratings.append(rating)
                genre_affinity[album['genre']].append(rating)
                artist_affinity[album['artistId']].append(rating)
    
    if album_song_ratings:
        album_ratings[album['albumId']] = sum(album_song_ratings) / len(album_song_ratings)
    
    # Prosečne ocene po žanrovima i artistima
    avg_genre_ratings = {}
    for genre, ratings_list in genre_affinity.items():
        avg_genre_ratings[genre] = sum(ratings_list) / len(ratings_list)
    
    avg_artist_ratings = {}
    for artist_id, ratings_list in artist_affinity.items():
        avg_artist_ratings[artist_id] = sum(ratings_list) / len(ratings_list)
    
    
    # Korak 3: Analiza istorije slušanja
    now = datetime.now()
    recent_threshold = now - timedelta(days=30)  # Skorašnja istorija
    
    # Učestalost žanrova i artista
    genre_frequency = Counter()
    artist_frequency = Counter()
    recent_genre_frequency = Counter()
    recent_artist_frequency = Counter()
    
    # Vremenska analiza (po satima dana)
    hourly_genre_preferences = defaultdict(Counter)
    current_hour = now.hour
    
    for entry in history:
        timestamp = datetime.fromisoformat(entry['timestamp'])
        genre = entry['genre']
        artist = entry['artist']
        
        genre_frequency[genre] += 1
        artist_frequency[artist] += 1
        
        # Skorašnja aktivnost (veći značaj)
        if timestamp >= recent_threshold:
            recent_genre_frequency[genre] += 2
            recent_artist_frequency[artist] += 2
        
        
        # Vremenska analiza - preferencije po satima
        hour = timestamp.hour
        hourly_genre_preferences[hour][genre] += 1
    
    
    # Korak 4: Računanje konačnog afiniteta za svaki album
    album_scores = {}
    
    
    for album in albums:
        
        score = 0
        album_id = album['albumId']
        genre = album['genre']
        artist_id = album['artistId']
        
        # 1. Direktan boost od pretplata
        score += subscription_boost.get(album_id, 0)
        
        # 2. Afinitet na osnovu direktnih ocena albuma
        if album_id in album_ratings:
            # Visoke ocene (4-5) daju pozitivan boost, niske (1-2) negativan
            album_rating = album_ratings[album_id]
            if album_rating >= 4:
                score += (album_rating - 3) * 20  # 20-40 bodova
            elif album_rating <= 2:
                score -= (3 - album_rating) * 15  # -15 do -30 bodova
        
        # 3. Afinitet prema žanru na osnovu istorijskih ocena
        if genre in avg_genre_ratings:
            genre_rating = avg_genre_ratings[genre]
            if genre_rating >= 3.5:
                score += (genre_rating - 3) * 15
            elif genre_rating <= 2.5:
                score -= (3 - genre_rating) * 10
        
        # 4. Afinitet prema artisti na osnovu istorijskih ocena
        if artist_id in avg_artist_ratings:
            artist_rating = avg_artist_ratings[artist_id]
            if artist_rating >= 3.5:
                score += (artist_rating - 3) * 25
            elif artist_rating <= 2.5:
                score -= (3 - artist_rating) * 15
        
        # 5. Učestalost slušanja žanra (ukupna + skorašnja)
        total_genre_plays = genre_frequency.get(genre, 0) + recent_genre_frequency.get(genre, 0)
        score += min(total_genre_plays * 2, 30)  # Maksimalno 30 bodova
        
        # 6. Učestalost slušanja artiste
        total_artist_plays = artist_frequency.get(artist_id, 0) + recent_artist_frequency.get(artist_id, 0)
        score += min(total_artist_plays * 3, 40)  # Maksimalno 40 bodova
        
        # 7. Vremenska analiza - bonus ako je žanr popularan u trenutno vreme dana
        if genre in hourly_genre_preferences.get(current_hour, {}):
            time_preference = hourly_genre_preferences[current_hour][genre]
            score += min(time_preference * 5, 25)  # Maksimalno 25 bodova
        
        # 8. Bonus za popularne žanrove u skorije vreme (trend detection)
        if recent_genre_frequency[genre] > genre_frequency[genre] * 0.3:
            score += 15  # Trending žanr bonus
        
        # 9. Penalty za žanrove koje korisnik izbegava
        if genre in avg_genre_ratings and avg_genre_ratings[genre] < 2:
            score -= 20
        
        # 10. Raznovrsnost - mala penalizacija za previše sličan sadržaj
        # (jednostavna implementacija - možda treba sofisticiraniji pristup)
        diversity_penalty = min(total_genre_plays * 0.5, 10)
        score = max(score - diversity_penalty, 0)
        
        album_scores[album_id] = max(score, 0)  # Ne dozvoljavamo negativne skorove
    
    
    # Korak 5: Sortiranje albuma po skoru
    sorted_albums = sorted(albums, 
                          key=lambda album: album_scores.get(album['albumId'], 0), 
                          reverse=True)
    
    sorted_albums = convert_decimals_to_float(sorted_albums)

    return sorted_albums


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

def _get_all_content(table):
    try:

        scan_kwargs = {
            
        }

        response = table.scan(**scan_kwargs)
        items = [_sanitize_item(item) for item in response.get('Items', [])]

        result = {
            'content': items,
            'count': len(items)
        }

        return {
            'statusCode': 200,
            'body': json.dumps(result)
        }
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
        'isExplicit': item.get('isExplicit', False)
    }
def _get_content_by_id(table, content_id, bucket_name):
    try:
        response = table.get_item(Key={'contentId': content_id})

        if 'Item' not in response:
            return {
                'statusCode': 404,
                'body': json.dumps({'error': 'Content not found'})
            }
        item = response['Item']

        stream_url = _generate_stream_url(item, bucket_name)
        safe_item = _sanitize_item(item)
        safe_item['streamUrl'] = stream_url

        return {
            'statusCode': 200,
            'body': json.dumps({'content': safe_item})
        }
    except Exception as e:
        print(f"Error fetching content by ID: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Failed to get content by ID'})
        }
    
def _generate_stream_url(item: Dict[str, Any], bucket_name: str, expires_in: int = 3600):
    try:
        persigned_url = s3_client.generate_presigned_url(
            'get_object',
            Params={
                'Bucket': bucket_name, 
                'Key': item['s3Key'],
                'ResponseContentType': item.get('fileType', 'audio/mpeg'),
                'ResponseContentDisposition': f'inline; filename="{item["filename"]}"'
            },
            ExpiresIn=expires_in
        )
        return persigned_url
    except Exception as e:
        print(f"Error generating presigned URL: {str(e)}")
        return ""

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

def _get_content_by_artist(artist_id, table, query_params):
    try:
        limit = min(int(query_params.get('limit', 50)), 100)
        last_key = query_params.get('lastKey')

        query_kwargs = {
            'IndexName': 'artistId-index',
            'KeyConditionExpression': 'artistId = :artistId',
            'ExpressionAttributeValues': {':artistId': artist_id},
            'Limit': limit
        }

        if last_key:
            query_kwargs['ExclusiveStartKey'] = { 'contentId': last_key }

        response = table.query(**query_kwargs)
        items = [_sanitize_item(item) for item in response.get('Items', [])]

        result = {
            'content': items,
            'count': len(items),
            'artistId': artist_id
        }

        if 'LastEvaluatedKey' in response:
            result['lastKey'] = response['LastEvaluatedKey']['contentId']

        return {
            'statusCode': 200,
            'body': json.dumps(result)
        }
    except Exception as e:
        print(f"Error fetching content by artist: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Failed to get content by artist'})
        }

def _search_content_by_title(search_query, table, query_params):
    try:
        limit = min(int(query_params.get('limit', 50)), 100)

        scan_kwargs = {
            'FilterExpression': 'contains(#title, :search)',
            'ExpressionAttributeNames': {'#title': 'title'},
            'ExpressionAttributeValues': [':search', search_query],
            'Limit': limit
        }

        response = table.scan(**scan_kwargs)
        items = [_sanitize_item(item) for item in response.get('Items', [])]

        result = {
            'content': items,
            'count': len(items),
            'searchQuery': search_query
        }

        if 'LastEvaluatedKey' in response:
            result['lastKey'] = response['LastEvaluatedKey']['contentId']

        return {
            'statusCode': 200,
            'body': json.dumps(result)
        }
    except Exception as e:
        print(f"Error searching content by title: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Failed to search content by title'})
        }



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

# def get_subscriptions(username):
#     """Get subscriptions from DynamoDB with optional pagination and filtering"""
#     try:
#         table = dynamodb.Table(os.environ['SUBSCRIPTIONS_TABLE'])
        
#             # Scan parameters
#         scan_params = {
            
#         }

#         scan_params['FilterExpression'] = 'contains(username, :username)'
#         scan_params['ExpressionAttributeValues'] = {':username': username}
        
        
#         # Perform scan
#         response = table.scan(**scan_params)
        
#         # Transform artists data for frontend
#         subscriptions = []
#         for item in response.get('Items', []):
#             subscription = transform_subscription_for_response(item)
#             subscriptions.append(subscription)
        
#         result = subscriptions
        
#         return result
        
#     except Exception as e:
#         logger.error(f"Error getting subscriptions: {str(e)}")
#         raise

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