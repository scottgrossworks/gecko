'''
THIS IS THE lambda_function.py code

Handles /publish

Sends newsletter to all subscribers in DynamoDB (production run)

No preview, no web rendering, no single-email outreach

'''

import os
import json
import boto3
import urllib.parse
import time
import logging
import re
from datetime import datetime
try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except ImportError:
    from backports.zoneinfo import ZoneInfo  # For local dev if needed

from botocore.exceptions import ClientError




# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
dynamodb = boto3.client('dynamodb')
ses = boto3.client('ses')
lambda_client = boto3.client('lambda')

# Environment variables
TABLE_NAME = os.environ.get('DDB_name', 'gecko_db')

RENDER_FUNCTION = os.environ.get('RENDER_FUNCTION', 'gecko_render')

GSI_NAME = os.environ.get('GSI', 'status-index')  # GSI name for querying subscribers
BATCH_SIZE = int(os.environ.get('BATCH_SIZE', '10'))  # Number of emails before sleep
SLEEP_TIME = float(os.environ.get('SLEEP_TIME', '1.0'))  # Sleep time in seconds

EMAIL_SOURCE = os.environ.get('EMAIL_SOURCE')
EMAIL_TARGET = os.environ.get('EMAIL_TARGET')
EMAIL_SUBJECT = os.environ.get('EMAIL_SUBJECT')

LIBRARY_LINK = os.environ.get('LIBRARY_LINK')
FAQ_LINK = os.environ.get('FAQ_LINK')

WIDTH = "'100%'"

UNSUBSCRIBE_BODY = (
    "Maybe it's a mistake, but I need a break from the flow of news and insights. "
    "Like Gordon Gekko at the end of Wall Street, I’m stepping away — for now. "
    "Please unsubscribe me from Gecko's Birthday."
    )

        
# CORS headers for all responses
CORS_HEADERS = {
    'Content-Type': 'application/json',
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization'
}


##
##
##
def format_date():
    """Format the current date for the newsletter"""
    return datetime.now().strftime("%B %d, %Y")



##
## Helper function -- return time-based greeting
##

def get_greeting():
    now = datetime.now(ZoneInfo("America/Los_Angeles"))
    hour = now.hour
    
    if 5 <= hour < 12:
        return "GOOD MORNING!"
    elif 12 <= hour < 17:
        return "GOOD AFTERNOON!"
    else:
        return "GOOD EVENING!"

##
## Helper function -- return the ASCII header
##


def getHeaderAscii():
    now = datetime.now(ZoneInfo("America/Los_Angeles"))
    time_str = now.strftime("%-I:%M %p").upper()
    date_str = now.strftime("%B %d, %Y - %A").upper()
    greeting = get_greeting()
    header_html = f"""
<table width="100%" cellpadding="0" cellspacing="0" border="0" style="background:black;">
  <tr>
    <td align="center" style="padding-bottom:10px;">
      <table width={WIDTH} cellpadding="6px" cellspacing="0" style="border:2px solid white; background:black;">
        <tr>
          <td style="padding: 12px 0 4px 0;"><center>
            <table width="90%" cellpadding="0" cellspacing="0" border="0">
              <tr>
                <td align="left" style="color: chartreuse; font-size: 1.2em; font-weight: bold; letter-spacing: 1.5px; font-family: Tahoma, Geneva, Verdana, sans-serif;">
                  {greeting}
                </td>
                <td align="right" style="color: chartreuse; font-size: 1.2em; font-weight: bold; letter-spacing: 1.5px; font-family: Tahoma, Geneva, Verdana, sans-serif;">
                  {time_str}
                </td>
              </tr>
            </table></center>
          </td>
        </tr>
        <tr>
          <td style="text-align: center; color: white; font-size: 1em; font-weight:600; padding: 10px 0 4px 0; letter-spacing: 1.1px; font-family: Tahoma, Geneva, Verdana, sans-serif;">
            {date_str}
          </td>
        </tr>
        <tr>
          <td align="center" style="padding: 12px 0 20px 0;">
            <div style="display: inline-block; border: 2px solid white; border-radius: 11px; padding: 3px; background: #111111;">
              <span style="display: inline-block; border: 2px solid white; color: red; background: #111111; font-size: 1.7em; font-weight: bold; border-radius: 7px; padding: 8px 20px; letter-spacing: 2px; box-shadow: 0 0 12px #000a; font-family: Tahoma, Geneva, Verdana, sans-serif;">
                GEKKO'S BIRTHDAY
              </span>
            </div>
          </td>
        </tr>
      </table>
    </td>
  </tr>
</table>
"""
    return header_html



##
##
##
def render_links( subscription_link ):

    #formatted_date = format_date()
    
    # Updated to match web version: larger text (1.1em) and "MBA Links" instead of "MBA Library"
    library_link = f"<a href='{LIBRARY_LINK}' target='_blank' style='color: gold; font-weight: bold; text-decoration: none; font-size: 1.1em;'>MBA Links</a>"
    faq_link = f"<a href='{FAQ_LINK}' target='_blank' style='color: white; font-weight: bold; text-decoration: none; font-size: 1.1em;'>FAQ</a>"

    # Center all links with GREEN pipes between them (matching web version)
    green_pipe = "<span style='color: chartreuse; font-weight: bold; font-size: 1.1em;'>&nbsp;&nbsp;|&nbsp;&nbsp;</span>"
    all_links = f"{library_link} {green_pipe} {faq_link} {green_pipe} {subscription_link}"

    header_html = f"""
<tr>
  <td style='letter-spacing: 1.1px; text-align: center; padding: 10px 0;'>
    {all_links}
  </td>
</tr>
"""
    return header_html




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
            ScanIndexForward=True,  # Ascending order by date (FIFO - oldest first)
            Limit=count
        )
        
        stories = response.get('Items', [])
        
        if not stories:
            logger.warning("No queued stories found in DynamoDB")
            return []
        

        
        # Ensure we only process the requested count
        stories = stories[:count]
        logger.info(f"Processing {len(stories)} stories (requested: {count})")
        
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
## Get stories without updating their status (for preview mode)
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
        # Query for top stories with status "queued"
        response = dynamodb.query(
            TableName=TABLE_NAME,
            IndexName=GSI_NAME,
            KeyConditionExpression='#status = :status',
            ExpressionAttributeNames={'#status': 'status'},
            ExpressionAttributeValues={':status': {'S': 'queued'}},
            ScanIndexForward=True,  # Ascending order by date (FIFO - oldest first)
            Limit=count
        )
        
        stories = response.get('Items', [])
        
        if not stories:
            logger.warning("No queued stories found in DynamoDB")
            return []
        

        
        # Ensure we only process the requested count
        stories = stories[:count]
        logger.info(f"Found {len(stories)} stories for preview (requested: {count})")
        
        return stories
        
    except ClientError as e:
        logger.error(f"Error fetching stories: {str(e)}")
        raise


##
## Send a single email (for preview mode)
##
def send_single_email(email, email_content, subject):
    """
    Send a single email to a recipient
    
    Args:
        email (str): Recipient email address
        email_content (str): HTML email content
        subject (str): Email subject line
        
    Returns:
        bool: True if successful
    """
    try:
        # Send email via SES
        ses.send_email(
            Source=EMAIL_SOURCE,
            Destination={'ToAddresses': [email]},
            Message={
                'Subject': {'Data': subject},
                'Body': {'Html': {'Data': email_content}}
            }
        )
        logger.info(f"Preview email sent to {email}")
        return True
        
    except ClientError as e:
        logger.error(f"Error sending preview email to {email}: {str(e)}")
        return False


##
## Render the email version of the newsletter
##
def render_email_version( stories, gecko_unsub ):

    # GET STANDARD HEADER HTML SAME FOR ALL VERSIONS
    header_html = getHeaderAscii()

     
    # Properly encode the body for mailto link
    body_text = UNSUBSCRIBE_BODY.replace('\n', ' ').replace('"', '%22')  # Replace newlines with spaces and escape quotes
    body_encoded = urllib.parse.quote(body_text, safe='')  # Encode everything
    
    mailto_unsub = (
        f"<a style='color:chartreuse;font-weight:600;font-size:1.1em;' href='mailto:{gecko_unsub}"
        f"?subject={urllib.parse.quote('Unsubscribe from Gekko\'s Birthday')}"
        f"&body={body_encoded}'>Unsubscribe</a>"
    )

    sub_html = render_links( mailto_unsub )

    header_html += sub_html

    footer_html = f"<br>{mailto_unsub}"


    # RENDER THE STORIES AS HTML STRING
    stories_html = render_stories(stories)


    ## COMPOSE THE COMPLETE HTML
    ##
    top_html = f"<!DOCTYPE html><html><head><meta charset='UTF-8'><title>Gekko's Birthday</title><style>body,html{{background-color:black;color:white;margin:0;padding:0;font-family:'Tahoma',monospace;}}</style></head>"
    
    body_html = f"<body bgcolor='black' text='white' link='white' alink='white' style='background-color:black;color:white;margin:0;padding:0;font-family:'Verdana',monospace;'><BR><BR> \
    <table width='100%' border='0' cellspacing='0' cellpadding='0' bgcolor='black'><tr><td align='center'><table width='600' border='0' cellspacing='0' cellpadding='20' bgcolor='black'> \
    <tr><td bgcolor='black' style='padding:15px;'><table width='100%' border='0' cellspacing='0' cellpadding='0' bgcolor='black' style='font-family:'Tahoma',monospace;'>{header_html}</table><BR><div style='margin:0 15px;'><font style='font-family:Helvetica, sans-serif; letter-spacing:1.25px;'>{stories_html}</font></div>"
    
    footer_html = f"<hr color='white' size='1' style='border:none;border-top:1px solid white;margin:20px 0;'><div align='center' style='color:chartreuse;font-size:12px;text-align:center;margin-top:20px;'>&copy; {datetime.now().year} GEKKO'S BIRTHDAY Newsletter, \
    produced by <a href='http://scottgross.works'>Scott Gross</a>. All rights reserved.<BR>{mailto_unsub}<BR></div></td></tr></table></td></tr></table><BR></body></html>"
    
    final_html = top_html + body_html + footer_html
    
    return final_html


##
## Render the email version with subscribe link (for preview mode)
##
def render_email_version_with_subscribe(stories, gecko_subscribe):
    """
    Render email version with subscribe link instead of unsubscribe
    
    Args:
        stories (list): List of story items
        gecko_subscribe (str): Subscribe email address
        
    Returns:
        str: Complete HTML email content
    """
    # GET STANDARD HEADER HTML SAME FOR ALL VERSIONS
    header_html = getHeaderAscii()

    # Create subscribe link instead of unsubscribe
    subscribe_body = (
        "I want news and insights at the intersection of business, technology, and culture. "
        "Like Bud Fox in Wall Street, start my day with the information that can change my life. "
        "Sign me up for the Gekko's Birthday Newsletter!"
    )
    
    # Properly encode the body for mailto link
    body_text = subscribe_body.replace('\n', ' ').replace('"', '%22')
    body_encoded = urllib.parse.quote(body_text, safe='')
    
    mailto_subscribe = (
        f"<a style='color:chartreuse;font-weight:600;font-size:1.1em;' href='mailto:{gecko_subscribe}"
        f"?subject={urllib.parse.quote('Subscribe to Gekko\'s Birthday')}"
        f"&body={body_encoded}'>Subscribe</a>"
    )

    sub_html = render_links(mailto_subscribe)
    header_html += sub_html
    footer_html = f"<br>{mailto_subscribe}"

    # RENDER THE STORIES AS HTML STRING
    stories_html = render_stories(stories)

    ## COMPOSE THE COMPLETE HTML
    top_html = f"<!DOCTYPE html><html><head><meta charset='UTF-8'><title>Gekko's Birthday</title><style>body,html{{background-color:black;color:white;margin:0;padding:0;font-family:'Tahoma',monospace;}}</style></head>"
    
    body_html = f"<body bgcolor='black' text='white' link='white' alink='white' style='background-color:black;color:white;margin:0;padding:0;font-family:'Verdana',monospace;'><BR><BR> \
    <table width='100%' border='0' cellspacing='0' cellpadding='0' bgcolor='black'><tr><td align='center'><table width='600' border='0' cellspacing='0' cellpadding='20' bgcolor='black'> \
    <tr><td bgcolor='black' style='padding:15px;'><table width='100%' border='0' cellspacing='0' cellpadding='0' bgcolor='black' style='font-family:'Tahoma',monospace;'>{header_html}</table><BR><div style='margin:0 15px;'><font style='font-family:Helvetica, sans-serif; letter-spacing:1.25px;'>{stories_html}</font></div>"
    
    footer_html = f"<hr color='white' size='1' style='border:none;border-top:1px solid white;margin:20px 0;'><div align='center' style='color:chartreuse;font-size:12px;text-align:center;margin-top:20px;'>&copy; {datetime.now().year} GEKKO'S BIRTHDAY Newsletter, \
    produced by <a href='http://scottgross.works'>Scott Gross</a>. All rights reserved.<BR>{mailto_subscribe}<BR></div></td></tr></table></td></tr></table><BR></body></html>"
    
    final_html = top_html + body_html + footer_html
    
    return final_html


##
## PASS TO RENDER_FUNCTION
##
def render_stories(stories):
    try:
            # Prepare payload for the render_email Lambda
            payload = {
                'stories': stories,
            }
            
            logger.info(f"Invoking {RENDER_FUNCTION} Lambda")
            
            # Invoke the render_email Lambda synchronously
            response = lambda_client.invoke(
                FunctionName=RENDER_FUNCTION,
                InvocationType='RequestResponse',
                Payload=json.dumps(payload)
            )
            
            # Parse the response
            response_payload = json.loads(response['Payload'].read().decode())
            
            if response['StatusCode'] != 200:
                logger.error(f"Error invoking {RENDER_FUNCTION} Lambda: {response_payload}")
                raise Exception(f"{RENDER_FUNCTION} Lambda returned status {response['StatusCode']}")
            
            # Extract the email content from the response
            html_content = response_payload.get('html_content')
            
            if not html_content:
                logger.error(f"No html content returned from {RENDER_FUNCTION} Lambda: {response_payload}")
                raise Exception(f"No html content returned from {RENDER_FUNCTION} Lambda")
            
            return html_content
            
    except Exception as e:
        logger.error(f"Error invoking {RENDER_FUNCTION} Lambda: {str(e)}")
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
            'KeyConditionExpression': '#status = :status',
            'FilterExpression': '#pk = :pk',
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




"""
A Better Heuristic for the Subject Line
Let’s extract the first 2–4 “good” words from the title, 
skipping things like “the”, “a”, “an”, and ignoring punctuation. 


Get the title string (if present).
Split into words.
Skip common stopwords at the start.
Take the next 2–4 words (configurable).
Join them for a concise subject.
Fallback to 'Newsletter' if not enough info.
"""
def extract_subject_snippet(title, num_words=3):
    if not title:
        return "Newsletter"
    # Remove punctuation
    cleaned = re.sub(r'[^\w\s]', '', title)
    words = cleaned.split()
    # List of common stopwords to skip at the beginning
    stopwords = {'the', 'a', 'an', 'this', 'that', 'these', 'those'}
    # Skip leading stopwords
    filtecoral = [w for w in words if w.lower() not in stopwords or words.index(w) > 0]
    # Take the first num_words after skipping stopwords
    snippet = " ".join(filtecoral[:num_words])
    return snippet if snippet else "Newsletter"




"""
Send personalized emails to all subscribers with rate limiting

Args:
    subscribers (list): List of subscriber items
    email_content (str): HTML email content with placeholder for email
    subject (str): Email subject line
    
Returns:
    int: Number of emails sent
"""
def send_emails_to_subscribers(subscribers, email_content, subject):

    sent_count = 0
    error_count = 0

    if not EMAIL_SOURCE:
        logger.error("EMAIL_SOURCE environment variable is not set")
        return 0
    


    for subscriber in subscribers:
        try:
            # Get subscriber email from sk field (sort key contains the email)
            email = subscriber.get('sk', {}).get('S', '')
            if not email:
                logger.warning(f"Subscriber missing sk (email) field: {subscriber}")
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
def lambda_handler(event, context):
    # Log all incoming requests for debugging
    logger.info(f"Incoming request - Method: {event.get('httpMethod')}, Path: {event.get('path')}, QueryParams: {event.get('queryStringParameters')}")

    # Handle preflight OPTIONS request
    if event.get('httpMethod') == 'OPTIONS':
        return {
            'statusCode': 200,
            'headers': CORS_HEADERS,
            'body': json.dumps({'message': 'CORS preflight'})
        }

    # Parse request data
    request_data = event
    if event.get('body'):
        try:
            request_data = json.loads(event['body'])
        except json.JSONDecodeError:
            request_data = event
    elif event.get('queryStringParameters'):
        request_data = event.get('queryStringParameters', {})
    
    # Determine mode: preview, single_shot, or broadcast (default)
    preview_mode = request_data.get('preview_mode') == 'true'
    single_shot_mode = request_data.get('email') and not preview_mode  # Has email but not preview
    single_shot_email = request_data.get('email') if single_shot_mode else None
    preview_email = request_data.get('email') if preview_mode else None
    
    if preview_mode:
        logger.info(f"Preview mode activated for: {preview_email}")
        if not preview_email:
            return {
                'statusCode': 400,
                'body': json.dumps({"message": "Missing email parameter for preview mode"}),
                'headers': CORS_HEADERS
            }
    elif single_shot_mode:
        logger.info(f"Single shot mode activated for: {single_shot_email}")
        if not single_shot_email:
            return {
                'statusCode': 400,
                'body': json.dumps({"message": "Missing email parameter for single shot mode"}),
                'headers': CORS_HEADERS
            }
    else:
        # Regular publish mode - check secret key
        secret_key = os.environ.get('SECRET_KEY')
        if not secret_key:
            return {
                'statusCode': 404,
                'body': json.dumps({"message": "No SECRET_KEY environment variable configured"}),
                'headers': CORS_HEADERS
            }
        
        # Check if this is a manual trigger with secret key
        if request_data.get('source') == 'manual' and request_data.get('secret_key') == secret_key:
            logger.info("Manual trigger activated!")
        elif event.get('source') == 'aws.events':  # EventBridge trigger
            logger.info("Scheduled trigger activated!")
        else:
            return {
                'statusCode': 404,
                'body': json.dumps({"message": "Invalid trigger source or secret key"}),
                'headers': CORS_HEADERS
            }

    try:
        # 1. Get stories (update status only in broadcast mode)
        if preview_mode or single_shot_mode:
            stories = get_stories_without_update(count=3)
        else:
            stories = get_and_update_stories(count=3)
        
        if not stories:
            if preview_mode or single_shot_mode:
                return {
                    'statusCode': 404,
                    'body': json.dumps({"message": "No stories available for newsletter"}),
                    'headers': CORS_HEADERS
                }
            else:
                return {
                    'statusCode': 200,
                    'body': json.dumps({
                        "message": "No new stories to publish",
                        "details": "All stories have already been published. Add new stories with status='queued' to publish a newsletter.",
                        "stories_count": 0,
                        "action_taken": "none"
                    }),
                    'headers': CORS_HEADERS
                }
        
        # 2. Render the email version with appropriate link
        if preview_mode:
            # Use subscribe link for preview
            gecko_subscribe_email = "gekko.subscribe@scottgross.works"
            email_content = render_email_version_with_subscribe(stories, gecko_subscribe_email)
            subject = os.environ.get('EMAIL_SUBJECT', "Gekko's Birthday * Preview")
        elif single_shot_mode:
            # Use unsubscribe link for single shot (like regular publish)
            gecko_link = EMAIL_TARGET  # Unsubscribe email
            email_content = render_email_version(stories, gecko_link)
            
            # Construct subject for single shot
            title_dict = stories[0].get('title', {})
            title_str = title_dict.get('S', '') if isinstance(title_dict, dict) else str(title_dict)
            subject_snippet = extract_subject_snippet(title_str, num_words=3)
            subject = f"Gecko's Birthday - {subject_snippet}"
        else:
            # Use unsubscribe link for broadcast publish
            gecko_link = EMAIL_TARGET  # Unsubscribe email
            email_content = render_email_version(stories, gecko_link)
            
            # Construct subject for broadcast publish
            title_dict = stories[0].get('title', {})
            title_str = title_dict.get('S', '') if isinstance(title_dict, dict) else str(title_dict)
            subject_snippet = extract_subject_snippet(title_str, num_words=3)
            subject = f"Gecko's Birthday - {subject_snippet}"
        
        # 3. Send emails
        if preview_mode:
            # Send single preview email
            success = send_single_email(preview_email, email_content, subject)
            return {
                'statusCode': 200,
                'body': json.dumps({
                    "message": f"Newsletter sent to {preview_email}",
                    "stories_count": len(stories)
                }),
                'headers': CORS_HEADERS
            }
        elif single_shot_mode:
            # Send single newsletter to new subscriber
            success = send_single_email(single_shot_email, email_content, subject)
            return {
                'statusCode': 200,
                'body': json.dumps({
                    "message": f"First newsletter sent to {single_shot_email}",
                    "stories_count": len(stories)
                }),
                'headers': CORS_HEADERS
            }
        else:
            # Send to all subscribers (broadcast mode)
            subscribers = get_subscribers()
            if not subscribers:
                return {
                    'statusCode': 404,
                    'body': json.dumps({"message": "No subscribed users found"}),
                    'headers': CORS_HEADERS
                }
            
            sent_count = send_emails_to_subscribers(subscribers, email_content, subject)
            return {
                'statusCode': 200,
                'body': json.dumps({
                    "message": f"Newsletter sent to {sent_count} recipients",
                    "stories_count": len(stories),
                    "recipients_count": len(subscribers),
                }),
                'headers': CORS_HEADERS
            }
        
    except Exception as e:
        logger.error(f"Error in gecko_publisher: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({"message": f"Error: {str(e)}"}),
            'headers': CORS_HEADERS
        }



        ##
        ## EOF gecko_publisher