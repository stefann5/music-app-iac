# lambda_functions/registration/index.py
import json
import boto3
import uuid
import os
from datetime import datetime
import logging

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
cognito_client = boto3.client('cognito-idp')
dynamodb = boto3.resource('dynamodb')

def handler(event, context):
    """
    User Registration Handler
    
    Implements requirement 1.1: Registracija korisnika
    Allows users to register with: firstName, lastName, dateOfBirth, 
    username (unique), email (unique), and password.
    """
    
    logger.info(f"Registration request received")
    
    try:
        # Parse request body
        if not event.get('body'):
            return create_error_response(400, "Request body is required")
        
        body = json.loads(event['body'])
        
        # Validate input according to requirements
        validation_result = validate_registration_input(body)
        if not validation_result['is_valid']:
            return create_error_response(400, "Validation failed", validation_result['errors'])
        
        # Check uniqueness constraints
        if check_username_exists(body['username']):
            return create_error_response(409, "Username already exists")
        
        if check_email_exists(body['email']):
            return create_error_response(409, "Email already exists")
        
        # Create user in Cognito
        cognito_user_id = create_cognito_user(body)
        
        # Store user profile in DynamoDB
        user_id = str(uuid.uuid4())
        store_user_profile(user_id, cognito_user_id, body)
        
        logger.info(f"User registered successfully: {user_id}")
        
        insert_empty_feed(body['username'])

        trigger_feed_calculation(
            username=body['username']
        )

        return create_success_response(201, {
            'message': 'User registered successfully',
            'userId': user_id,
            'username': body['username'],
            'email': body['email']
        })
        
    except ValueError as e:
        logger.error(f"Validation error: {str(e)}")
        return create_error_response(400, str(e))
    
    except Exception as e:
        logger.error(f"Registration error: {str(e)}")
        return create_error_response(500, "Internal server error")

def trigger_feed_calculation(username):
    """Trigger feed calculation after subscription update"""
    
    lambda_client = boto3.client('lambda')
    
    payload = {
        'username': username,
        'action': 'registration',
        'timestamp': datetime.now().isoformat()
    }
    
    # Invoke calculate feed function asynchronously
    lambda_client.invoke(
        FunctionName=os.environ['CALCULATE_FEED_FUNCTION'],
        InvocationType='Event',  # Async invocation
        Payload=json.dumps(payload)
    )
    
    print(f"Feed calculation triggered for user: {username}")

def validate_registration_input(input_data):
    """
    Validate registration input according to requirements:
    - ime (firstName) - required
    - prezime (lastName) - required  
    - datum rođenja (dateOfBirth) - required
    - korisničko ime (username) - required, unique
    - email - required, unique, valid format
    - lozinka (password) - required, minimum length
    """
    errors = []
    
    # Required fields validation
    required_fields = ['firstName', 'lastName', 'username', 'email', 'password', 'dateOfBirth']
    for field in required_fields:
        if not input_data.get(field) or not str(input_data[field]).strip():
            errors.append(f'{field} is required')
    
    # First name validation
    if input_data.get('firstName') and len(input_data['firstName'].strip()) < 2:
        errors.append('First name must be at least 2 characters')
    
    # Last name validation
    if input_data.get('lastName') and len(input_data['lastName'].strip()) < 2:
        errors.append('Last name must be at least 2 characters')
    
    # Username validation
    if input_data.get('username'):
        username = input_data['username'].strip()
        if len(username) < 3:
            errors.append('Username must be at least 3 characters')
        if not username.replace('_', '').replace('-', '').isalnum():
            errors.append('Username must contain only letters, numbers, hyphens, and underscores')
    
    # Email validation
    if input_data.get('email') and not is_valid_email(input_data['email']):
        errors.append('Valid email address is required')
    
    # Password validation
    if input_data.get('password'):
        min_length = int(os.environ.get('PASSWORD_MIN_LENGTH', 8))
        password = input_data['password']
        if len(password) < min_length:
            errors.append(f'Password must be at least {min_length} characters')
        if not any(c.islower() for c in password):
            errors.append('Password must contain at least one lowercase letter')
        if not any(c.isupper() for c in password):
            errors.append('Password must contain at least one uppercase letter')
        if not any(c.isdigit() for c in password):
            errors.append('Password must contain at least one number')
    
    # Date of birth validation
    if input_data.get('dateOfBirth') and not is_valid_date(input_data['dateOfBirth']):
        errors.append('Valid date of birth is required (YYYY-MM-DD format)')
    
    return {
        'is_valid': len(errors) == 0,
        'errors': errors
    }

def is_valid_email(email):
    """Validate email format"""
    import re
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(pattern, email) is not None

def is_valid_date(date_string):
    """Validate date format and check if it's a reasonable birth date"""
    try:
        birth_date = datetime.strptime(date_string, '%Y-%m-%d')
        
        # Check if date is not in the future
        if birth_date > datetime.now():
            return False
            
        # Check if date is not too far in the past (reasonable birth year)
        current_year = datetime.now().year
        if birth_date.year < current_year - 120:  # 120 years old maximum
            return False
            
        return True
    except ValueError:
        return False

def check_username_exists(username):
    """Check if username already exists in the system"""
    try:
        table = dynamodb.Table(os.environ['USERS_TABLE'])
        response = table.query(
            IndexName='username-index',
            KeyConditionExpression='username = :username',
            ExpressionAttributeValues={':username': username}
        )
        return len(response['Items']) > 0
    except Exception as e:
        logger.error(f"Error checking username: {str(e)}")
        return False

def check_email_exists(email):
    """Check if email already exists in the system"""
    try:
        table = dynamodb.Table(os.environ['USERS_TABLE'])
        response = table.query(
            IndexName='email-index',
            KeyConditionExpression='email = :email',
            ExpressionAttributeValues={':email': email}
        )
        return len(response['Items']) > 0
    except Exception as e:
        logger.error(f"Error checking email: {str(e)}")
        return False

def create_cognito_user(user_data):
    """Create user in Cognito User Pool"""
    try:
        # Create user in Cognito
        response = cognito_client.admin_create_user(
            UserPoolId=os.environ['USER_POOL_ID'],
            Username=user_data['username'],  # Use email as username
            UserAttributes=[
                {'Name': 'email', 'Value': user_data['email']},
                {'Name': 'given_name', 'Value': user_data['firstName']},
                {'Name': 'family_name', 'Value': user_data['lastName']},
                {'Name': 'birthdate', 'Value': user_data['dateOfBirth']},
                {'Name': 'preferred_username', 'Value': user_data['username']},
                {'Name': 'custom:role', 'Value': 'user'},
                {'Name': 'custom:subscription_type', 'Value': 'free'}
            ],
            TemporaryPassword=user_data['password'],
            MessageAction='SUPPRESS'  # Don't send welcome email
        )
        
        cognito_user_id = response['User']['Username']
        
        # Set permanent password
        cognito_client.admin_set_user_password(
            UserPoolId=os.environ['USER_POOL_ID'],
            Username=cognito_user_id,
            Password=user_data['password'],
            Permanent=True
        )
        
        # Add user to default "users" group
        try:
            cognito_client.admin_add_user_to_group(
                UserPoolId=os.environ['USER_POOL_ID'],
                Username=cognito_user_id,
                GroupName='users'
            )
        except Exception as e:
            logger.warning(f"Could not add user to group: {str(e)}")
        
        return cognito_user_id
        
    except Exception as e:
        logger.error(f"Error creating Cognito user: {str(e)}")
        raise ValueError(f"Failed to create user account: {str(e)}")

def store_user_profile(user_id, cognito_user_id, user_data):
    """Store user profile data in DynamoDB"""
    try:
        table = dynamodb.Table(os.environ['USERS_TABLE'])
        
        item = {
            'userId': user_id,
            'cognitoUserId': cognito_user_id,
            'username': user_data['username'],
            'email': user_data['email'],
            'firstName': user_data['firstName'],
            'lastName': user_data['lastName'],
            'dateOfBirth': user_data['dateOfBirth'],
            'role': 'user',
            'subscriptionType': 'free',
            'status': 'active',
            'createdAt': datetime.utcnow().isoformat(),
            'lastLogin': None,
            'preferences': {
                'genres': [],
                'notifications': {
                    'email': True,
                    'newContent': True,
                    'subscriptions': True
                },
                'privacy': {
                    'profileVisibility': 'public',
                    'showListeningHistory': True
                }
            },
            'stats': {
                'songsPlayed': 0,
                'llisteningHistory': [],
                'totalListeningTime': 0,
                'favoriteGenres': [],
                'joinDate': datetime.utcnow().isoformat()
            }
        }
        
        table.put_item(Item=item)
        logger.info(f"User profile stored: {user_id}")
        
    except Exception as e:
        logger.error(f"Error storing user profile: {str(e)}")
        raise

def insert_empty_feed(username):
    """Insert new user feed with empty list"""
    try:
        dynamodb = boto3.resource('dynamodb')
        feed_table = dynamodb.Table(os.environ['FEED_TABLE'])
        
        item = {
            'username': username,           # PK
            'feed': [],                     # Empty list
        }
        
        # Insert item
        feed_table.put_item(Item=item)
        
        print(f"Empty feed created for user: {username}")
        return item
        
    except Exception as e:
        print(f"Error creating feed for {username}: {str(e)}")
        raise

def create_success_response(status_code, data):
    """Create standardized success response"""
    return {
        'statusCode': status_code,
        'headers': get_cors_headers(),
        'body': json.dumps(data, default=str)
    }

def create_error_response(status_code, message, details=None):
    """Create standardized error response"""
    error_data = {
        'error': message,
        'timestamp': datetime.utcnow().isoformat()
    }
    if details:
        error_data['details'] = details
    
    return {
        'statusCode': status_code,
        'headers': get_cors_headers(),
        'body': json.dumps(error_data)
    }

def get_cors_headers():
    """Get CORS headers for API responses"""
    return {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type,Authorization,X-Requested-With',
        'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',
        'Content-Type': 'application/json'
    }