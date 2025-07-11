'''
THIS IS THE lambda_function.py code

Fetches and updates stories:
Queries DynamoDB for top stories with "queued" status
Updates them to "published" status with timestamp
Uses batch operations for efficiency

Renders email content by invoking the render_email Lambda:
Calls the existing render_email Lambda function
Uses a placeholder for the recipient email

Fetches subscribers:
Gets all users with "subscribed" status
Handles pagination for large subscriber lists

Sends personalized emails:
Replaces the placeholder with each recipient's email
Implements rate limiting with sleep()
Handles SES errors and throttling with retries
Logs progress and results

Configuration via environment variables:
Table names, API endpoint, email source
Batch size and sleep time for rate limiting
'''
import os
import json
import boto3
import time
import logging
from datetime import datetime
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize AWS clients
dynamodb = boto3.client('dynamodb')
ses = boto3.client('ses')
lambda_client = boto3.client('lambda')

# Environment variables
TABLE_NAME = os.environ.get('DDB_name', 'gecko_db')
API_ENDPOINT = os.environ.get('GECKO_API', 'https://api.scottgross.works/subscribe')
EMAIL_SOURCE = os.environ.get('EMAIL_SOURCE', 'gecko@scottgross.works')
RENDER_EMAIL_FUNCTION = os.environ.get('RENDER_EMAIL_FUNCTION', 'gecko_email_render')
GSI_NAME = os.environ.get('GSI', 'status-index')  # GSI name for querying subscribers
BATCH_SIZE = int(os.environ.get('BATCH_SIZE', '10'))  # Number of emails before sleep
SLEEP_TIME = float(os.environ.get('SLEEP_TIME', '1.0'))  # Sleep time in seconds


##
##
##
def get_and_update_stories(count=3):
    """
    Fetch top stories from DynamoDB and update their status to published
    
    Args:
        count (int): Number of stories to fetch
        
    Returns:
        list: List of story items in DynamoDB format
    """
    try:
        # Query for top stories with status "queued"
        response = dynamodb.query(
            TableName=TABLE_NAME,
            IndexName=GSI_NAME,
            KeyConditionExpression='#status = :status',
            ExpressionAttributeNames={'#status': 'status'},
            ExpressionAttributeValues={':status': {'S': 'queued'}},
            ScanIndexForward=False,  # Descending order by date
            Limit=count
        )
        
        stories = response.get('Items', [])
        
        if not stories:
            logger.warning("No queued stories found in DynamoDB")
            return []
        
        # Update stories with published status
        now = datetime.utcnow().isoformat() + 'Z'
        batch_items = []
        
        for story in stories:
            # Create a copy of the story with updated fields
            updated_story = story.copy()
            updated_story['published_date'] = {'S': now}
            updated_story['status'] = {'S': 'published'}
            
            # Add to batch write request
            batch_items.append({'PutRequest': {'Item': updated_story}})
        
        # Write back to DynamoDB
        if batch_items:
            dynamodb.batch_write_item(RequestItems={TABLE_NAME: batch_items})
            logger.info(f"Updated {len(batch_items)} stories to published status")
        
        return stories
        
    except ClientError as e:
        logger.error(f"Error fetching/updating stories: {str(e)}")
        raise



##
##
##
def invoke_render_email(stories, api_endpoint, recipient_email, recipient_status):
    """
    Invoke the render_email Lambda function to generate the email content
    
    Args:
        stories (list): List of story items in DynamoDB format
        api_endpoint (str): API endpoint for subscription management
        recipient_email (str): Recipient email address (or placeholder)
        recipient_status (str): Recipient subscription status
        
    Returns:
        str: Rendered HTML email content
    """
    try:
        # Prepare payload for the render_email Lambda
        payload = {
            'stories': stories,
            'api_endpoint': api_endpoint,
            'recipient': {
                'email': recipient_email,
                'status': recipient_status
            }
        }
        
        logger.info(f"Invoking {RENDER_EMAIL_FUNCTION} Lambda")
        
        # Invoke the render_email Lambda synchronously
        response = lambda_client.invoke(
            FunctionName=RENDER_EMAIL_FUNCTION,
            InvocationType='RequestResponse',
            Payload=json.dumps(payload)
        )
        
        # Parse the response
        response_payload = json.loads(response['Payload'].read().decode())
        
        if response['StatusCode'] != 200:
            logger.error(f"Error invoking {RENDER_EMAIL_FUNCTION} Lambda: {response_payload}")
            raise Exception(f"{RENDER_EMAIL_FUNCTION} Lambda returned status {response['StatusCode']}")
        
        # Extract the email content from the response
        email_content = response_payload.get('email_content')
        
        if not email_content:
            logger.error(f"No email content returned from {RENDER_EMAIL_FUNCTION} Lambda: {response_payload}")
            raise Exception(f"No email content returned from {RENDER_EMAIL_FUNCTION} Lambda")
        
        return email_content
        
    except Exception as e:
        logger.error(f"Error invoking {RENDER_EMAIL_FUNCTION} Lambda: {str(e)}")
        raise




##
##
##
def get_subscribers():
    """
    Fetch all subscribers with status "subscribed" from DynamoDB
    Uses the status-index GSI to efficiently query instead of scanning
    
    Returns:
        list: List of subscriber items
    """
    try:
        subscribers = []
        paginator = dynamodb.get_paginator('query')
        
        # Parameters for querying the status-index GSI
        query_params = {
            'TableName': TABLE_NAME,
            'IndexName': GSI_NAME,
            'KeyConditionExpression': '#status = :status AND #pk = :pk',
            'ExpressionAttributeNames': {'#status': 'status', '#pk': 'pk'},
            'ExpressionAttributeValues': {
                ':status': {'S': 'subscribed'},
                ':pk': {'S': 'user'}
            }
        }
        
        # Paginate through results
        for page in paginator.paginate(**query_params):
            subscribers.extend(page.get('Items', []))
        
        logger.info(f"Found {len(subscribers)} subscribed users using GSI: {GSI_NAME}")
        return subscribers
        
    except ClientError as e:
        logger.error(f"Error fetching subscribers: {str(e)}")
        raise



##
##
##
def send_emails_to_subscribers(subscribers, email_content, subject):
    """
    Send personalized emails to all subscribers with rate limiting
    
    Args:
        subscribers (list): List of subscriber items
        email_content (str): HTML email content with placeholder for email
        subject (str): Email subject line
        
    Returns:
        int: Number of emails sent
    """
    sent_count = 0
    error_count = 0
    
    for subscriber in subscribers:
        try:
            # Get subscriber email
            email = subscriber.get('email', {}).get('S', '')
            if not email:
                logger.warning(f"Subscriber missing email field: {subscriber}")
                continue
            
            # Personalize email content
            personalized_content = email_content.replace("PLACEHOLDER", email)
            
            # Send email via SES
            ses.send_email(
                Source=EMAIL_SOURCE,
                Destination={'ToAddresses': [email]},
                Message={
                    'Subject': {'Data': subject},
                    'Body': {'Html': {'Data': personalized_content}}
                }
            )
            
            sent_count += 1
            
            # Rate limiting - sleep every BATCH_SIZE emails
            if sent_count % BATCH_SIZE == 0:
                logger.info(f"Sent {sent_count} emails, sleeping for {SLEEP_TIME}s")
                time.sleep(SLEEP_TIME)
                
        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_count += 1
            
            if error_code == 'MessageRejected':
                logger.warning(f"Email to {email} was rejected: {str(e)}")
            elif error_code == 'Throttling':
                # If throttled, wait longer and retry
                logger.warning(f"Throttling detected, sleeping for {SLEEP_TIME * 2}s")
                time.sleep(SLEEP_TIME * 2)
                
                # Retry sending this email
                try:
                    ses.send_email(
                        Source=EMAIL_SOURCE,
                        Destination={'ToAddresses': [email]},
                        Message={
                            'Subject': {'Data': subject},
                            'Body': {'Html': {'Data': personalized_content}}
                        }
                    )
                    sent_count += 1
                except Exception as retry_err:
                    logger.error(f"Retry failed for {email}: {str(retry_err)}")
            else:
                logger.error(f"Error sending to {email}: {str(e)}")
    
    logger.info(f"Email sending complete. Sent: {sent_count}, Errors: {error_count}")
    return sent_count



##
##
##
def get_stories_without_update(count=3):
    """
    Fetch top stories from DynamoDB without updating their status
    
    Args:
        count (int): Number of stories to fetch
        
    Returns:
        list: List of story items in DynamoDB format
    """
    try:
        # Query for top stories with no published_date
        response = dynamodb.query(
            TableName=TABLE_NAME,
            KeyConditionExpression='#pk = :pk',
            ExpressionAttributeNames={'#pk': 'pk'},
            ExpressionAttributeValues={':pk': {'S': 'story'}},
            ScanIndexForward=False,  # Descending order by sort key
            Limit=count
        )
        
        stories = response.get('Items', [])
        
        if not stories:
            logger.warning("No stories found in DynamoDB")
            
        return stories
        
    except ClientError as e:
        logger.error(f"Error fetching stories: {str(e)}")
        raise





##
##
##
def lambda_handler(event, context):
    try:
        # Check if this is a view-only request
        view_only = False
        if isinstance(event, dict) and event.get('queryStringParameters', {}) and event.get('queryStringParameters', {}).get('view') == 'true':
            view_only = True
            logger.info("View-only mode requested")
        
        # Check if this is a direct email request
        single_recipient = None
        if isinstance(event, dict) and 'email' in event.get('queryStringParameters', {}):
            single_recipient = event['queryStringParameters']['email']
            logger.info(f"Single recipient mode for: {single_recipient}")
        
        # 1. Get top stories and update their status (only update if not view-only)
        stories = get_and_update_stories(count=3) if not view_only else get_stories_without_update(count=3)
        
        if not stories:
            return {
                'statusCode': 404,
                'body': json.dumps({"message": "No stories available for newsletter"}),
                'headers': {'Content-Type': 'application/json'}
            }
        
        # 2. Invoke render_email Lambda to generate email content
        recipient_status = "new" if view_only or single_recipient else "subscribed"
        email_content = invoke_render_email(
            stories, 
            API_ENDPOINT, 
            single_recipient or "PLACEHOLDER",
            recipient_status
        )
        
        # If view-only, return the HTML directly
        if view_only:
            return {
                'statusCode': 200,
                'body': email_content,
                'headers': {'Content-Type': 'text/html'}
            }
        
        # 3. Get subscribers or use provided email
        if single_recipient:
            # Create a mock subscriber item for the single recipient
            subscribers = [{
                'email': {'S': single_recipient},
                'status': {'S': 'preview'}
            }]
        else:
            # Get all subscribed users
            subscribers = get_subscribers()
            
            if not subscribers:
                return {
                    'statusCode': 404,
                    'body': json.dumps({"message": "No subscribed users found"}),
                    'headers': {'Content-Type': 'application/json'}
                }
        
        # 4. Send emails with rate limiting
        subject = f"Gecko's Birthday - {stories[0].get('title', {}).get('S', 'Newsletter')}"
        sent_count = send_emails_to_subscribers(subscribers, email_content, subject)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                "message": f"Newsletter sent to {sent_count} recipients",
                "stories_count": len(stories),
                "recipients_count": len(subscribers),
                "single_mode": single_recipient is not None
            }),
            'headers': {'Content-Type': 'application/json'}
        }
        
    except Exception as e:
        logger.error(f"Error in gecko_publisher: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({"message": f"Error: {str(e)}"}),
            'headers': {'Content-Type': 'application/json'}
        }



        ##
        ## EOF gecko_publisher