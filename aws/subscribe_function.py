# subscriber_function.py
#
# PROCESS SUBSCRIBER REGISTRATION, UNSUBSCRIBE  REQUESTS
# STORE IN DDB gecko_db
# RETURN 200 SUCCESS!

import json
import boto3
from botocore.exceptions import ClientError
import logging
from datetime import datetime
import os
import uuid
import hashlib
# import base64
import re



# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
dynamodb = boto3.client('dynamodb')
TABLE_NAME = os.environ.get('DDB_NAME')

# Constants
ACTION_SUBSCRIBE = 'subscribe'
ACTION_UNSUBSCRIBE = 'unsubscribe'

STATUS_SUBSCRIBED = 'subscribed'
STATUS_UNSUBSCRIBED = 'unsubscribed'

EMAIL_REGEX = r"^[\w\.-]+@[\w\.-]+\.\w+$"

PUBLISHER_FUNCTION = os.environ.get('PUBLISHER_FUNCTION')


##
##
##
def hash_password(password):
    """Simple password hashing for demonstration purposes.
    In production, use a proper password hashing library."""
    salt = uuid.uuid4().hex
    hashed = hashlib.sha256(salt.encode() + password.encode()).hexdigest()
    return f"{salt}:{hashed}"


##
##
##
def process_subscribe(body, email):
    """Process a subscription request"""
    logger.info(f"Processing subscription for: {email}")
    
    # Generate timestamp
    now = datetime.utcnow().isoformat() + 'Z'
    
    # Build subscriber item with required fields
    item = {
        'pk': {'S': 'user'},
        'sk': {'S': email},
        'date_created': {'S': now},
        'status': {'S': STATUS_SUBSCRIBED}
    }
    
    # Add optional fields if present
    if 'name' in body and body['name']:
        item['name'] = {'S': body['name'].strip()}
        
    if 'password' in body and body['password']:
        item['password'] = {'S': hash_password(body['password'])}
        
    if 'zip_code' in body and body['zip_code']:
        item['zip_code'] = {'S': body['zip_code'].strip()}
        
    if 'interests' in body and isinstance(body['interests'], list) and body['interests']:
        item['interests'] = {'L': [{'S': interest.strip()} for interest in body['interests']]}
        
    # Additional optional fields
    for field in ['subscription_tier', 'preferences', 'referral_source']:
        if field in body and body[field]:
            if isinstance(body[field], dict):
                # Convert dict to DynamoDB M type
                item[field] = {'M': {k: {'S': str(v)} for k, v in body[field].items()}}
            else:
                item[field] = {'S': str(body[field])}
    
    # Check if subscriber already exists
    try:
        # Validate environment
        if not TABLE_NAME:
            raise ValueError("DDB_NAME environment variable is not set")

        existing = dynamodb.get_item(
            TableName=TABLE_NAME,
            Key={
                'pk': {'S': 'user'},
                'sk': {'S': email}
            }
        )
        
        if 'Item' in existing:
            logger.info(f"Subscriber already exists, updating: {email}")
            # Preserve the original date_created if it exists
            if 'date_created' in existing['Item']:
                item['date_created'] = existing['Item']['date_created']
            
    except ClientError as e:
        logger.error(f"Error checking for existing subscriber: {str(e)}")
        # Continue with registration attempt
    
    # Save to DynamoDB (creates new or overwrites existing)
    dynamodb.put_item(TableName=TABLE_NAME, Item=item)
    
    logger.info(f"Subscriber registered/updated: {email}")
    
    # Check if we should send first newsletter issue
    send_first_issue = body.get('send_first_issue', 'false').lower() == 'true'
    
    if send_first_issue:
        try:
            lambda_client = boto3.client('lambda')
            
            # Call publisher in single_shot mode
            payload = {
                'email': email
            }
            
            response = lambda_client.invoke(
                FunctionName=PUBLISHER_FUNCTION,
                InvocationType='Event',  # Async
                Payload=json.dumps(payload)
            )
            
            logger.info(f"First newsletter sent to: {email}")
            
            # Update user record to mark first issue as sent
            dynamodb.update_item(
                TableName=TABLE_NAME,
                Key={
                    'pk': {'S': 'user'},
                    'sk': {'S': email}
                },
                UpdateExpression='SET first_issue_sent = :val',
                ExpressionAttributeValues={
                    ':val': {'S': datetime.utcnow().isoformat() + 'Z'}
                }
            )
            
            return {
                'message': 'Subscription successful and first newsletter sent',
                'email': email,
                'status': STATUS_SUBSCRIBED,
                'first_issue_sent': True
            }
            
        except Exception as e:
            logger.error(f"Error sending first newsletter to {email}: {str(e)}")
            # Still return success for subscription, just note the newsletter issue
            return {
                'message': 'Subscription successful but first newsletter failed to send',
                'email': email,
                'status': STATUS_SUBSCRIBED,
                'newsletter_error': str(e)
            }
    
    return {
        'message': 'Subscription successful',
        'email': email,
        'status': STATUS_SUBSCRIBED
    }



##
##
##
def process_unsubscribe(email):
    """Process an unsubscribe request"""
    logger.info(f"Processing unsubscribe for: {email}")
    
    # Check if subscriber exists
    try:
        existing = dynamodb.get_item(
            TableName=TABLE_NAME,
            Key={
                'pk': {'S': 'user'},
                'sk': {'S': email}
            }
        )
        
        if 'Item' not in existing:
            logger.warning(f"Unsubscribe request for non-existent subscriber: {email}")
            return {
                'message': 'Email not found in subscriber list',
                'email': email
            }
        
        # Instead of UpdateItem, use PutItem to update the status
        # Preserve all existing attributes
        item = existing['Item']
        item['status'] = {'S': STATUS_UNSUBSCRIBED}
        item['date_updated'] = {'S': datetime.utcnow().isoformat() + 'Z'}
        
        # Save to DynamoDB
        dynamodb.put_item(TableName=TABLE_NAME, Item=item)
        
        logger.info(f"Subscriber unsubscribed: {email}")
        
        return {
            'message': 'Unsubscribe successful',
            'email': email,
            'status': STATUS_UNSUBSCRIBED
        }
        
    except ClientError as e:
        logger.error(f"Error processing unsubscribe: {str(e)}")
        raise e



##
##
  
def lambda_handler(event, context):
    try:

        if not PUBLISHER_FUNCTION:
            raise ValueError("PUBLISHER_FUNCTION environment variable is not set")

        logger.info(f"Subscriber Event received: {json.dumps(event)}")

        # Check if this is an SES email event
        if 'Records' in event and event['Records']:
            # Handle SES email event (unsubscribe emails)
            record = event['Records'][0]
            if record.get('eventSource') == 'aws:ses':
                # Extract email from SES event
                ses_mail = record['ses']['mail']
                from_email = ses_mail['commonHeaders']['from'][0]
                
                # Extract just the email address from "Name <email@domain.com>" format
                email_match = re.search(r'<([^>]+)>', from_email)
                if email_match:
                    email = email_match.group(1).strip().lower()
                else:
                    # If no angle brackets, assume the whole string is the email
                    email = from_email.strip().lower()
                
                # Check destination to determine action
                destination = ses_mail['destination'][0]
                if 'unsubscribe' in destination.lower():
                    action = ACTION_UNSUBSCRIBE
                else:
                    action = ACTION_SUBSCRIBE
                
                logger.info(f"SES email event - Email: {email}, Action: {action}")
                
                # Validate email
                if not re.match(EMAIL_REGEX, email):
                    raise ValueError(f"Invalid email format: {email}")
                
                # Process the action
                if action == ACTION_UNSUBSCRIBE:
                    result = process_unsubscribe(email)
                else:
                    result = process_subscribe({}, email)
                
                return {
                    'statusCode': 200,
                    'body': json.dumps(result)
                }
        
        # Handle API Gateway request
        # Extract params (API Gateway GET or direct test)
        params = event.get('queryStringParameters', {}) or {}

        # EMAIL
        if 'email' not in params:
            raise ValueError("Email is required")
        email = params['email'].strip().lower()
        if not email:
            raise ValueError("Email cannot be empty")
        if not re.match(EMAIL_REGEX, email):
            raise ValueError("Invalid email format")

        # ACTION
        action = params.get('action', ACTION_SUBSCRIBE).lower()

        # TAGS/INTERESTS (future-proof)
        interests = params.get('interests')
        if interests is None:
            tags = []
        elif isinstance(interests, list):
            tags = interests
        elif isinstance(interests, str):
            # Support comma-separated string for web/query param
            tags = [t.strip() for t in interests.split(",") if t.strip()]
        else:
            tags = []

        # Add tags to params for downstream processing
        params['tags'] = tags

        # DISPATCH
        if action == ACTION_SUBSCRIBE:
            result = process_subscribe(params, email)
        elif action == ACTION_UNSUBSCRIBE:
            result = process_unsubscribe(email)
        else:
            raise ValueError(f"Invalid action: {action}. Must be one of: {ACTION_SUBSCRIBE}, {ACTION_UNSUBSCRIBE}")

        # RESPONSE
        return {
            'statusCode': 200,
            'body': json.dumps(result),
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Content-Type': 'application/json'
            }
        }

    except ValueError as e:
        error_msg = f"Validation Error: {str(e)}"
        logger.error(error_msg)
        return {
            'statusCode': 400,
            'body': json.dumps({'error': error_msg}),
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Content-Type': 'application/json'
            }
        }
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        logger.error(error_msg)
        return {
            'statusCode': 500,
            'body': json.dumps({'error': error_msg}),
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Content-Type': 'application/json'
            }
        }
# EOF subscribe_function.py