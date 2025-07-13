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
    Registration handler - implements requirement 1.1
    Korisnik se registruje tako što unosi ime, prezime, datum rođenja, 
    korisničko ime i email (moraju biti jedinstveni), i lozinku.
    """
    
    logger.info(f"Registration request received for stage: {os.environ.get('STAGE')}")
    
    try:
        # Parse request body
        if not event.get('body'):
            return create_error_response(400, "Request body is required")
        
        body = json.loads(event['body'])
        
        # Validate input (requirement 1.1)
        validation_result = validate_registration_input(body)
        if not validation_result['is_valid']:
            return create_error_response(400, "Validation failed", validation_result['errors'])
        
        # Check if username is unique (requirement 1.1)
        if check_username_exists(body['username']):
            return create_error_response(409, "Username already exists")
        
        # Check if email is unique (requirement 1.1)
        if check_email_exists(body['email']):
            return create_error_response(409, "Email already exists")
        
        # Create user in Cognito
        cognito_user_id = create_cognito_user(body)
        
        # Store additional user data in DynamoDB
        user_id = str(uuid.uuid4())
        store_user_profile(user_id, cognito_user_id, body)
        
        logger.info(f"User registered successfully: {user_id}")
        
        return create_success_response(201, {
            'message': 'User registered successfully',
            'userId': user_id
        })
        
    except ValueError as e:
        logger.error(f"Validation error: {str(e)}")
        return create_error_response(400, str(e))
    
    except Exception as e:
        logger.error(f"Registration error: {str(e)}")
        return create_error_response(500, "Internal server error")

def validate_registration_input(input_data):
    """
    Validate registration input according to requirement 1.1:
    ime, prezime, datum rođenja, korisničko ime i email (moraju biti jedinstveni), i lozinku
    """
    errors = []
    
    # Required fields per specification
    required_fields = ['firstName', 'lastName', 'username', 'email', 'password', 'dateOfBirth']
    for field in required_fields:
        if not input_data.get(field) or not str(input_data[field]).strip():
            errors.append(f'{field} is required')
    
    # Validate firstName (ime)
    if input_data.get('firstName') and len(input_data['firstName'].strip()) < 2:
        errors.append('First name must be at least 2 characters')
    
    # Validate lastName (prezime)
    if input_data.get('lastName') and len(input_data['lastName'].strip()) < 2:
        errors.append('Last name must be at least 2 characters')
    
    # Validate username (korisničko ime)
    if input_data.get('username'):
        username = input_data['username'].strip()
        if len(username) < 3:
            errors.append('Username must be at least 3 characters')
        if not username.isalnum():
            errors.append('Username must contain only letters and numbers')
    
    # Validate email
    if input_data.get('email') and not is_valid_email(input_data['email']):
        errors.append('Valid email is required')
    
    # Validate password (lozinka)
    if input_data.get('password'):
        min_length = int(os.environ.get('PASSWORD_MIN_LENGTH', 8))
        if len(input_data['password']) < min_length:
            errors.append(f'Password must be at least {min_length} characters')
    
    # Validate dateOfBirth (datum rođenja)
    if input_data.get('dateOfBirth') and not is_valid_date(input_data['dateOfBirth']):
        errors.append('Valid date of birth is required (YYYY-MM-DD format)')
    
    return {
        'is_valid': len(errors) == 0,
        'errors': errors
    }

def is_valid_email(email):
    """Simple email validation"""
    import re
    pattern = r'^[^\s@]+@[^\s@]+\.[^\s@]+$'
    return re.match(pattern, email) is not None

def is_valid_date(date_string):
    """Validate date format"""
    try:
        datetime.strptime(date_string, '%Y-%m-%d')
        return True
    except ValueError:
        return False

def check_username_exists(username):
    """Check if username already exists (requirement 1.1 - unique username)"""
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
    """Check if email already exists (requirement 1.1 - unique email)"""
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
            Username=user_data['email'],  # Use email as username
            UserAttributes=[
                {'Name': 'email', 'Value': user_data['email']},
                {'Name': 'given_name', 'Value': user_data['firstName']},
                {'Name': 'family_name', 'Value': user_data['lastName']},
                {'Name': 'birthdate', 'Value': user_data['dateOfBirth']},
                {'Name': 'preferred_username', 'Value': user_data['username']},
                {'Name': 'custom:role', 'Value': 'user'}
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
        
        # Add user to "users" group
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
    """Store additional user profile data in DynamoDB"""
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
            'status': 'active',
            'createdAt': datetime.utcnow().isoformat(),
            'lastLogin': None,
            'preferences': {
                'notifications': True,
                'privacy': 'public'
            }
        }
        
        table.put_item(Item=item)
        logger.info(f"User profile stored: {user_id}")
        
    except Exception as e:
        logger.error(f"Error storing user profile: {str(e)}")
        raise

def create_success_response(status_code, data):
    """Create standardized success response"""
    return {
        'statusCode': status_code,
        'headers': get_cors_headers(),
        'body': json.dumps(data)
    }

def create_error_response(status_code, message, details=None):
    """Create standardized error response"""
    error_data = {'error': message}
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
        'Access-Control-Allow-Headers': 'Content-Type,Authorization',
        'Access-Control-Allow-Methods': 'POST,OPTIONS',
        'Content-Type': 'application/json'
    }