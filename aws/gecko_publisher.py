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
EMAIL_UNSUB = os.environ.get('EMAIL_UNSUB')
EMAIL_SUBJECT = os.environ.get('EMAIL_SUBJECT')
EMAIL_SUBSCRIBE = os.environ.get('EMAIL_SUBSCRIBE')

LIBRARY_LINK = os.environ.get('LIBRARY_LINK')
FAQ_LINK = os.environ.get('FAQ_LINK')

WIDTH = "'100%'"

UNSUBSCRIBE_BODY = (
    "Maybe it's a mistake, but I need a break from the flow of news and insights. "
    "Like Gordon Gekko at the end of Wall Street, I’m stepping away — for now. "
    "Please unsubscribe me from Gecko's Birthday."
    )

# Create subscribe link instead of unsubscribe
SUBSCRIBE_BODY = (
    "I want news and insights at the intersection of business, technology, and culture. "
    "Like Bud Fox in Wall Street, start my day with the information that can change my life. "
    "Sign me up for the Gekko's Birthday Newsletter!"
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





"""
    Fetch top stories from DynamoDB without updating their status
    
    Args:
        count (int): Number of stories to fetch
        
    Returns:
        list: List of story items in DynamoDB format
"""
def get_stories_without_update(count=3):
   
    try:
        # Query for most recently published stories (newest first for preview)
        response = dynamodb.query(
            TableName=TABLE_NAME,
            IndexName='status-index',  # Use GSI to filter by status
            KeyConditionExpression='#status = :status',
            ExpressionAttributeNames={'#status': 'status'},
            ExpressionAttributeValues={':status': {'S': 'published'}},
            ScanIndexForward=False,  # Descending order by date (newest first)
            Limit=count
        )
        
        stories = response.get('Items', [])
        
        if not stories:
            logger.warning("No published stories found in DynamoDB")
            
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
## Render a refresh newsletter
## this version:  has a plaintext intro BEFORE the header_html with an refresh msg
## personalized link to Email <mailto:scottgrossworks@gmail.com>Scott Gross</a>
## THe intro is white background, same as default
## 50px margin 
## then the standard email content generted by render_email_version
##
## refresh text:
## Dear friend, 
## I noticed you didn't Subscibe to Gekko's Birthday from the preview I sent you.
## <BR>
## I also notice you didn't get rich like me, Gordon Gekko.  
## I'm talking talking *liquid*.  Rich enough to have your own jet.
## Rich enough not to waste time.
## $50, $100 million dollars, buddy.  A player.
## <BR>
## So, I'm sending you another chance to subscribe to the news and insights that
## could change your life.
## <BR>
## To get in touch, Email my friend <mailto:scottgrossworks@gmail.com>Scott Gross</a>
## 




##
##
##


"""
Render refresh newsletter with white background intro and standard email content

Args:
    stories (list): List of story items
    gecko_unsub (str): Unsubscribe email address
    
Returns:
    str: Complete HTML email content with refresh intro
"""
##
## Render the refresh newsletter version
## White background intro with Gekko-style follow-up message, then standard content
##
def render_refresh_version(stories):

    # GET STANDARD HEADER HTML SAME FOR ALL VERSIONS
    header_html = getHeaderAscii()

    # Properly encode the body for mailto link
    body_text = SUBSCRIBE_BODY.replace('\n', ' ').replace('"', '%22')
    body_encoded = urllib.parse.quote(body_text, safe='')
    
    mailto_subscribe = (
        f"<a style='color:chartreuse;font-weight:600;font-size:1.1em;' href='mailto:{EMAIL_SUBSCRIBE}"
        f"?subject={urllib.parse.quote('Subscribe to Gekko\'s Birthday')}"
        f"&body={body_encoded}'>Subscribe</a>"
    )

    # White background refresh intro with exact text and formatting
    refresh_intro = (
        "<div style='background-color: white; color: black; padding: 20px; margin: 10px 0; font-family: Helvetica, sans-serif; font-size: 14px; line-height: 1.4;'>"
        "Dear friend,<BR><BR>"
        "I noticed you didn't Subscribe to Gekko's Birthday from the preview I sent you.&nbsp;&nbsp;"
        "I also notice you didn't get rich like me, Gordon Gekko.&nbsp;&nbsp;"
        "I'm talking talking *liquid*. Rich enough to have your own jet.&nbsp;&nbsp;"
        "Rich enough not to waste time.&nbsp;&nbsp;"
        "$50, $100 million dollars, buddy. A player.<BR><BR>"
        "So, I'm sending you another chance to " + mailto_subscribe + " to the news and insights that "
        "could change your life.<BR><BR>"
        "To get in touch, email my friend <a href='mailto:scottgrossworks@gmail.com' style='color: blue; text-decoration: underline;'>Scott Gross</a>"
        "</div>"
    )


    sub_html = render_links(mailto_subscribe)

    header_html += sub_html

    # RENDER THE STORIES AS HTML STRING
    stories_html = render_stories(stories)

    ## COMPOSE THE COMPLETE HTML WITH REFRESH INTRO
    top_html = f"<!DOCTYPE html><html><head><meta charset='UTF-8'><title>Gekko's Birthday - Second Chance</title><style>body,html{{background-color:black;color:white;margin:0;padding:0;font-family:'Tahoma',monospace;}}</style></head>"
    
    # Body HTML with refresh intro inserted before the standard content
    body_html = f"<body bgcolor='black' text='white' link='white' alink='white' style='background-color:black;color:white;margin:0;padding:0;font-family:'Verdana',monospace;'>{refresh_intro}<BR><BR> \
    <table width='100%' border='0' cellspacing='0' cellpadding='0' bgcolor='black'><tr><td align='center'><table width='600' border='0' cellspacing='0' cellpadding='20' bgcolor='black'> \
    <tr><td bgcolor='black' style='padding:15px;'><table width='100%' border='0' cellspacing='0' cellpadding='0' bgcolor='black' style='font-family:'Tahoma',monospace;'>{header_html}</table><BR><div style='margin:0 15px;'><font style='font-family:Helvetica, sans-serif; letter-spacing:1.25px;'>{stories_html}</font></div>"
    
    footer_html = f"<hr color='white' size='1' style='border:none;border-top:1px solid white;margin:20px 0;'><div align='center' style='color:chartreuse;font-size:12px;text-align:center;margin-top:20px;'>&copy; {datetime.now().year} GEKKO'S BIRTHDAY Newsletter, \
    produced by <a href='http://scottgross.works' style='color:red'>Scott Gross</a>. All rights reserved.<BR>{mailto_subscribe}<BR></div></td></tr></table></td></tr></table><BR></body></html>"
    
    final_html = top_html + body_html + footer_html

    # DEBUG: 
    # logger.info(f"DEBUG: final_html = {final_html}")


    return final_html















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

    # RENDER THE STORIES AS HTML STRING
    stories_html = render_stories(stories)


    ## COMPOSE THE COMPLETE HTML
    ##
    top_html = f"<!DOCTYPE html><html><head><meta charset='UTF-8'><title>Gekko's Birthday</title><style>body,html{{background-color:black;color:white;margin:0;padding:0;font-family:'Tahoma',monospace;}}</style></head>"
    
    body_html = f"<body bgcolor='black' text='white' link='white' alink='white' style='background-color:black;color:white;margin:0;padding:0;font-family:'Verdana',monospace;'><BR><BR> \
    <table width='100%' border='0' cellspacing='0' cellpadding='0' bgcolor='black'><tr><td align='center'><table width='600' border='0' cellspacing='0' cellpadding='20' bgcolor='black'> \
    <tr><td bgcolor='black' style='padding:15px;'><table width='100%' border='0' cellspacing='0' cellpadding='0' bgcolor='black' style='font-family:'Tahoma',monospace;'>{header_html}</table><BR><div style='margin:0 15px;'><font style='font-family:Helvetica, sans-serif; letter-spacing:1.25px;'>{stories_html}</font></div>"
    
    footer_html = f"<hr color='white' size='1' style='border:none;border-top:1px solid white;margin:20px 0;'><div align='center' style='color:chartreuse;font-size:12px;text-align:center;margin-top:20px;'>&copy; {datetime.now().year} GEKKO'S BIRTHDAY Newsletter, \
    produced by <a href='http://scottgross.works' style='color:red'>Scott Gross</a>. All rights reserved.<BR>{mailto_unsub}<BR></div></td></tr></table></td></tr></table><BR></body></html>"
    
    final_html = top_html + body_html + footer_html
    
    return final_html


##
## Render the email version with subscribe link (for preview mode)
##
def render_email_version_with_subscribe(stories, gecko_subscribe, intro=None):
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

    # Properly encode the body for mailto link
    body_text = SUBSCRIBE_BODY.replace('\n', ' ').replace('"', '%22')
    body_encoded = urllib.parse.quote(body_text, safe='')
    
    mailto_subscribe = (
        f"<a style='color:chartreuse;font-weight:600;font-size:1.1em;' href='mailto:{gecko_subscribe}"
        f"?subject={urllib.parse.quote('Subscribe to Gekko\'s Birthday')}"
        f"&body={body_encoded}'>Subscribe</a>"
    )



    # INTRO TEXT
    # If intro is provided, it will be added as a white background section
    # This allows for a personalized message before the main content
    # If no intro is provided, it will be an empty string
    # If intro is provided, add it as a white background section
    intro_html = ""
    if intro:
        intro_html = (
            f"<div style='background-color: white; color: black; padding: 20px; margin: 10px 0; font-family: Helvetica, sans-serif; font-size: 14px; line-height: 1.4;'>"
            f"{intro}<BR>{mailto_subscribe} to the news and insights that could change your life.<BR><BR>"
        f"To get in touch, email my friend <a href='mailto:scottgrossworks@gmail.com' style='color:red; text-decoration: underline;'>Scott Gross</a>"
        "</div>"
        )



    sub_html = render_links(mailto_subscribe)
    header_html += sub_html

    # Create subscribe link instead of unsubscribe


    # RENDER THE STORIES AS HTML STRING
    stories_html = render_stories(stories)

    ## COMPOSE THE COMPLETE HTML
    top_html = f"<!DOCTYPE html><html><head><meta charset='UTF-8'><title>Gekko's Birthday</title><style>body,html{{background-color:black;color:white;margin:0;padding:0;font-family:'Tahoma',monospace;}}</style></head>"
    
    body_html = f"<body bgcolor='black' text='white' link='white' alink='white' style='background-color:black;color:white;margin:0;padding:0;font-family:'Verdana',monospace;'>{intro_html}<BR><BR> \
    <table width='100%' border='0' cellspacing='0' cellpadding='0' bgcolor='black'><tr><td align='center'><table width='600' border='0' cellspacing='0' cellpadding='20' bgcolor='black'> \
    <tr><td bgcolor='black' style='padding:15px;'><table width='100%' border='0' cellspacing='0' cellpadding='0' bgcolor='black' style='font-family:'Tahoma',monospace;'>{header_html}</table><BR><div style='margin:0 15px;'><font style='font-family:Helvetica, sans-serif; letter-spacing:1.25px;'>{stories_html}</font></div>"
    
    footer_html = f"<hr color='white' size='1' style='border:none;border-top:1px solid white;margin:20px 0;'><div align='center' style='color:chartreuse;font-size:12px;text-align:center;margin-top:20px;'>&copy; {datetime.now().year} GEKKO'S BIRTHDAY Newsletter, \
    produced by <a href='http://scottgross.works' style='color:red'>Scott Gross</a>. All rights reserved.<BR>{mailto_subscribe}<BR></div></td></tr></table></td></tr></table><BR></body></html>"
    
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
            
            # logger.info(f"Invoking {RENDER_FUNCTION} Lambda")
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
def get_non_subscribers():
    """
    Fetch all non-subscribers with status=null (not subscribed, not unsubscribed) 
    from DynamoDB
    Uses the status-index GSI to efficiently query instead of scanning

    Returns:
        list: List of non-subscriber items
    """
    try:
        non_subscribers = []
        paginator = dynamodb.get_paginator('query')

        # Parameters for querying the status-index GSI
        query_params = {
            'TableName': TABLE_NAME,
            'IndexName': GSI_NAME,
            'KeyConditionExpression': '#status = :status',
            'FilterExpression': '#pk = :pk',
            'ExpressionAttributeNames': {'#status': 'status', '#pk': 'pk'},
            'ExpressionAttributeValues': {
                ':status': {'S': 'new'},
                ':pk': {'S': 'user'}
            }
        }

        # Paginate through results
        for page in paginator.paginate(**query_params):
            non_subscribers.extend(page.get('Items', []))

        logger.info(f"Found {len(non_subscribers)} non-subscribed users using GSI: {GSI_NAME}")
        return non_subscribers

    except ClientError as e:
        logger.error(f"Error fetching non-subscribers: {str(e)}")
        raise


##
## so we don't annoy people with the refresh -- 
## this allows us to mark them once as refreshed
## will *not* get picked up by get_non_subscribers() above
##
def mark_refreshed( non_subscribers ):
    """
    Mark non-subscribers as refreshed in DynamoDB by updating their status to 'refreshed'
    
    Args:
        non_subscribers (list): List of non-subscriber items to update
        
    Returns:
        int: Number of items updated
    """
    try:
        if not non_subscribers:
            logger.info("No non-subscribers to mark as refreshed")
            return 0
        
        batch_items = []
        for subscriber in non_subscribers:
            # Create a copy of the subscriber with updated status
            updated_subscriber = subscriber.copy()
            updated_subscriber['status'] = {'S': 'refreshed'}
            
            # Add to batch write request
            batch_items.append({'PutRequest': {'Item': updated_subscriber}})
        
        # Write back to DynamoDB
        if batch_items:
            dynamodb.batch_write_item(RequestItems={TABLE_NAME: batch_items})
            logger.info(f"Marked {len(batch_items)} non-subscribers as refreshed")
            return len(batch_items)
        else:
            logger.info("No items to update in batch write")
            return 0
            
    except ClientError as e:
        logger.error(f"Error marking non-subscribers as refreshed: {str(e)}")
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
def extract_subject_snippet(title, num_words=5):
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

    the_email = None
    refresh_mode = False
    preview_mode = False    

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

        # THIS MAY BE NONE
        the_email = request_data.get('email')


    ##
    ## IS THIS A TEST EVENT?
    ##
    elif event.get('queryStringParameters'):
        try:
            ## DETECT REFRESH MODE WITH EMAIL PARAMETER HERE
            request_data = event.get('queryStringParameters', {})
            
            # Check for refresh mode with email parameter in test event
            if request_data.get('refresh') == 'true':
                refresh_mode = True
    
            elif request_data.get('preview') == 'true':
                # we are testing the preview mode
                preview_mode = True

            # TARGET EMAIL TO SEND NEWSLETTER TO            
            # THIS MAY BE NONE
            the_email = request_data.get('email')


        except (AttributeError, TypeError) as e:
            logger.error(f"Malformed queryStringParameters: {e}")
            return {
                'statusCode': 400,
                'body': json.dumps({"message": "Malformed queryStringParameters"}),
                'headers': CORS_HEADERS
            }



    # Determine mode: preview, single_shot, or broadcast (default)
    refresh_mode = refresh_mode or (request_data.get('refresh') == 'true')
    preview_mode = preview_mode or (request_data.get('preview') == 'true')
    single_shot_mode = the_email and not preview_mode  # Has email but not preview


    # otherwise we are in broadcast mode

    
    if refresh_mode:

        # preview_mode = True  # Refresh mode acts like preview mode
        if the_email:
            logger.info(f"Refresh test mode detected with email: {the_email}")       
        
        else:
            return {
                'statusCode': 400,
                'body': json.dumps({"message": "Missing email parameter for refresh test mode"}),
                'headers': CORS_HEADERS
            }

    elif preview_mode:
        
        if the_email:
            logger.info(f"Preview mode activated for: {the_email}")
        else:
            return {
                'statusCode': 400,
                'body': json.dumps({"message": "Missing email parameter for preview mode"}),
                'headers': CORS_HEADERS
            }
        
    elif single_shot_mode:
        
        if the_email:
            logger.info(f"Single shot mode activated for: {the_email}")
        else:
            return {
                'statusCode': 400,
                'body': json.dumps({"message": "Missing email parameter for single shot mode"}),
                'headers': CORS_HEADERS
            }
    
    
    ##
    else:
        # the publisher can be ONLY triggered IF the request includes SECRET_KEY    
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
        if refresh_mode or preview_mode or single_shot_mode:
            stories = get_stories_without_update(count=3)
        else:
            stories = get_and_update_stories(count=3)
        
        if not stories:
            if refresh_mode or preview_mode or single_shot_mode:
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
        

        ##
        ## 2. Render the email version with appropriate link
        ##
        if refresh_mode:
            # Use refresh version with subscribe link
            email_content = render_refresh_version(stories)
            subject = "Lunch is for Wimps - 2nd Chance to Subscribe"

        
        elif preview_mode:

            # is there intro text?
            intro_text = request_data.get('intro', None)
            email_content = render_email_version_with_subscribe(stories, EMAIL_SUBSCRIBE, intro_text)
            subject = os.environ.get('EMAIL_SUBJECT', "Gekko's Birthday * Preview")



        elif single_shot_mode:
            # Use unsubscribe link for single shot (like regular publish)
            email_content = render_email_version(stories, EMAIL_UNSUB)
            
            # Construct subject for single shot
            title_dict = stories[0].get('title', {})
            title_str = title_dict.get('S', '') if isinstance(title_dict, dict) else str(title_dict)
            subject_snippet = extract_subject_snippet(title_str, num_words=5)
            subject = subject_snippet
        
        else:
            # Use unsubscribe link for broadcast publish
            email_content = render_email_version(stories, EMAIL_UNSUB)

            # Construct subject for broadcast publish
            title_dict = stories[0].get('title', {})
            title_str = title_dict.get('S', '') if isinstance(title_dict, dict) else str(title_dict)
            subject_snippet = extract_subject_snippet(title_str, num_words=5)
            subject = subject_snippet
        

        ##
        ## 3. Send emails
        ##
        
        ## REFRESH MODE
        if refresh_mode:
            # Check if this is a test mode (has email parameter)

            # test_email WILL be set here if coming from test event
            if the_email:
                # TEST MODE: Send single-shot email for refresh testing
                success = send_single_email(the_email, email_content, subject)
            
                return {
                    'statusCode': 200,
                    'body': json.dumps({
                        "message": f"Refresh newsletter TEST sent to {the_email}",
                        "stories_count": len(stories),
                        "mode": "refresh_test"
                    }),
                    'headers': CORS_HEADERS
                }
            

            else:
                # REFRESH ALL NON-SUBSCRIBERS
                # Send to all non-subscribers (broadcast mode)
                subscribers = get_non_subscribers()
                if not subscribers:
                    return {
                        'statusCode': 404,
                        'body': json.dumps({"message": "No non-subscribed users found"}),
                        'headers': CORS_HEADERS
                    }
                else:
                    # Mark non-subscribers as refreshed
                    mark_refreshed(subscribers)
                    logger.info(f"Marked {len(subscribers)} non-subscribers as refreshed")


                sent_count = send_emails_to_subscribers(subscribers, email_content, subject)
                return {
                    'statusCode': 200,
                    'body': json.dumps({
                        "message": f"Newsletter sent to {sent_count} non-subscribers",
                        "stories_count": len(stories),
                        "recipients_count": len(subscribers),
                    }),
                    'headers': CORS_HEADERS
                }
        


        elif preview_mode:
            # Send single preview email
            success = send_single_email(the_email, email_content, subject)
            return {
                'statusCode': 200,
                'body': json.dumps({
                    "message": f"Newsletter sent to {the_email}",
                    "stories_count": len(stories)
                }),
                'headers': CORS_HEADERS
            }
        

        elif single_shot_mode:
            # Send single newsletter to new subscriber
            success = send_single_email(the_email, email_content, subject)
            return {
                'statusCode': 200,
                'body': json.dumps({
                    "message": f"First newsletter sent to {the_email}",
                    "stories_count": len(stories)
                }),
                'headers': CORS_HEADERS
            }
        
        ##
        ## BROADCAST MODE TO ALL SUBSCRIBERS
        ##
        else:
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
            'headers': CORS_HEADERS##
        }



        ##
        ## EOF gecko_publisher