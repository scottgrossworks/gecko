'''
## gecko_preview.py
## LIGHTWEIGHT ROUTER for preview functionality
## 
## Handles /preview requests:
## - No params → Forward to gecko_web.py for browser view
## - ?email=xxx → Forward to gecko_publisher.py in preview mode
##
## This eliminates code duplication by using gecko_publisher.py as the unified email engine
'''

import os
import json
import boto3
import logging
from datetime import datetime
from botocore.exceptions import ClientError

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
lambda_client = boto3.client('lambda')
dynamodb = boto3.client('dynamodb')

# Environment variables
WEB_FUNCTION = os.environ.get('WEB_FUNCTION', 'gecko_web')
TABLE_NAME = os.environ.get('DDB_NAME', 'gecko_db')




def create_preview_user(email, name=None, zip_code=None, interests=None):
    """Create a preview user profile with status: null (not subscribed yet)"""
    logger.info(f"Creating preview user profile for: {email}")
    
    # Generate timestamp
    now = datetime.utcnow().isoformat() + 'Z'
    
    # Check if user already exists - FETCH FIRST to preserve existing data
    try:
        existing = dynamodb.get_item(
            TableName=TABLE_NAME,
            Key={
                'pk': {'S': 'user'},
                'sk': {'S': email}
            }
        )
        
        if 'Item' in existing:
            logger.info(f"Preview user already exists, updating: {email}")
            # START WITH EXISTING ITEM - PRESERVE ALL EXISTING DATA
            item = existing['Item'].copy()
        else:
            logger.info(f"Creating new preview user: {email}")
            # NEW USER - Build base item
            item = {
                'pk': {'S': 'user'},
                'sk': {'S': email},
                'date_created': {'S': now},
                'status': {'S': 'new'}  # EXPLICITLY SET STATUS TO 'new'                
            }
            
    except ClientError as e:
        logger.error(f"Error checking for existing user: {str(e)}")
        # Create new user if fetch fails
        item = {
            'pk': {'S': 'user'},
            'sk': {'S': email},
            'date_created': {'S': now},
            'status': {'S': 'new'}  # EXPLICITLY SET STATUS TO 'new'
        }
    
    # ENSURE STATUS IS SET FOR EXISTING USERS TOO
    if 'status' not in item:
        item['status'] = {'S': 'new'}
        logger.info(f"Setting missing status field to 'new' for existing user: {email}")

    # Update ONLY the fields provided in this request (don't obliterate others)
    if name and name.strip():
        item['name'] = {'S': name.strip()}
        logger.info(f"Updating name for {email}")
        
    if zip_code and zip_code.strip():
        item['zip_code'] = {'S': zip_code.strip()}
        logger.info(f"Updating zip_code for {email}")
        
    if interests:
        if isinstance(interests, str):
            # Convert comma-separated string to list
            interests_list = [interest.strip() for interest in interests.split(',') if interest.strip()]
        else:
            interests_list = interests
            
        if interests_list:
            item['interests'] = {'L': [{'S': interest} for interest in interests_list]}
            logger.info(f"Updating interests for {email}: {interests_list}")
    
    # Save to DynamoDB (preserves all existing fields not updated)
    try:
        dynamodb.put_item(TableName=TABLE_NAME, Item=item)
        logger.info(f"Preview user profile created/updated: {email}")
        return True
    except ClientError as e:
        logger.error(f"Error creating preview user: {str(e)}")
        return False


def pass_to_web():
    """Forward request to gecko_web.py for browser HTML rendering"""
    try:
        response = lambda_client.invoke(
            FunctionName=WEB_FUNCTION,
            InvocationType='RequestResponse',
            Payload=json.dumps({})
        )
        
        response_payload = json.loads(response['Payload'].read())
        return response_payload
        
    except Exception as e:
        logger.error(f"Error invoking {WEB_FUNCTION}: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({"message": f"Error forwarding to web: {str(e)}"}),
            'headers': {'Content-Type': 'application/json'}
        }


def lambda_handler(event, context):
    """
    Lightweight router for preview functionality:
    - No params → Forward to gecko_web.py for browser view
    - ?email=xxx → Forward to gecko_publisher.py in preview mode
    """
    try:
        # NO QUERY PARAMS --> Forward to web for browser HTML
        if not event.get('queryStringParameters'):
            return pass_to_web()

        # Check if this is an email preview request
        query_params = event.get('queryStringParameters', {})
        single_recipient = query_params.get('email')
        
        if not single_recipient:
            logger.error("Missing email parameter")
            return {
                'statusCode': 400,
                'body': json.dumps({"message": "Missing email parameter"}),
                'headers': {'Content-Type': 'application/json'}
            }

        logger.info(f"Processing preview request for: {single_recipient}")
        
        # Step 1: Create/update preview user profile (status: null)
        # Always create user profile for preview - this is the main purpose
        try:
            success = create_preview_user(
                email=single_recipient,
                name=query_params.get('name'),
                zip_code=query_params.get('zip'),
                interests=query_params.get('interests')
            )
            if not success:
                logger.warning("Failed to create preview user profile, continuing with email")
        except Exception as e:
            logger.error(f"Error creating preview user profile: {str(e)}")
            # Continue anyway - don't fail preview if user creation fails
        
        # Step 2: Send preview email via publisher
        try:
            publisher_payload = {
                'queryStringParameters': {
                    'preview_mode': 'true',
                    'email': single_recipient
                }
            }
            
            response = lambda_client.invoke(
                FunctionName='gecko_publisher',
                InvocationType='RequestResponse',
                Payload=json.dumps(publisher_payload)
            )
            
            response_payload = json.loads(response['Payload'].read())
            return response_payload
            
        except Exception as e:
            logger.error(f"Error invoking gecko_publisher: {str(e)}")
            return {
                'statusCode': 500,
                'body': json.dumps({"message": f"Error forwarding to publisher: {str(e)}"}),
                'headers': {'Content-Type': 'application/json'}
            }
        
    except Exception as e:
        logger.error(f"Error in gecko_preview: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({"message": f"Error: {str(e)}"}),
            'headers': {'Content-Type': 'application/json'}
        }


# EOF gecko_preview.py

