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
EMAIL_SUBJECT = os.environ.get('EMAIL_SUBJECT', "Gekko's Birthday * Newsletter")

LIBRARY_LINK = os.environ.get('LIBRARY_LINK')
FAQ_LINK = os.environ.get('FAQ_LINK')

WIDTH = "'100%'"

UNSUBSCRIBE_BODY = (
    "Maybe it's a mistake, but I need a break from the flow of news and insights. "
    "Like Gordon Gekko at the end of Wall Street, I’m stepping away — for now. "
    "Please unsubscribe me from Gecko's Birthday."
    )




##
##
##
def format_date():
    """Format the current date for the newsletter"""
    return datetime.now().strftime("%B %d, %Y")



##
## Helper function -- return the ASCII header
##


def getHeaderAscii():
    now = datetime.now(ZoneInfo("America/Los_Angeles"))
    time_str = now.strftime("%-I:%M %p").upper()
    date_str = now.strftime("%B %d, %Y - %A").upper()
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
                  <img src="data:image/x-icon;base64,AAABAAEAEBAAAAEAIABoBAAAFgAAACgAAAAQAAAAIAAAAAEAIAAAAAAAAAQAABILAAASCwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA4KCgv+CgoL/goKC/4KCgv+CgoL/goKC/4KCgv+CgoL/goKC/4KCgv+CgoL/goKC/4KCgv8AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABra2v/a2tr/2tra/9ra2v/a2tr/2tra/9ra2v/a2tr/2tra/9ra2v/a2tr/2tra/9ra2v/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABkZGT/ZGRk/2RkZP9kZGT/ZGRk/2RkZP9kZGT/ZGRk/2RkZP9kZGT/ZGRk/2RkZP9kZGT/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACBgYH/gYGB/4GBgf+BgYH/gYGB/4GBgf+BgYH/gYGB/4GBgf+BgYH/gYGB/4GBgf+BgYH/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=" style="height: 16px; width: 16px; margin-right: 10px;" alt="SGW"> GOOD MORNING!
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
            <span style="display: inline-block; border: 2px solid white; color: coral; background: #111111; font-size: 1.7em; font-weight: bold; border-radius: 5px; padding: 8px 20px; letter-spacing: 2px; font-family: Tahoma, Geneva, Verdana, sans-serif;">
              GEKKO'S BIRTHDAY
            </span>
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
    
    library_link = f"<a href='{LIBRARY_LINK}' target='_blank' style='color: gold; font-weight: bold; text-decoration: none; font-size: 1em;'>MBA Links</a>"
    faq_link = f"<a href='{FAQ_LINK}' target='_blank' style='color: white; font-weight: bold; text-decoration: none; font-size: 1em;'>FAQ</a>"

    header_html = f"""
<tr>
  <td style='letter-spacing: 1.1px;'><center>
    <table width="{WIDTH}" cellpadding="0" cellspacing="0" border="0">
      <tr>
        <td align="left" style="padding: 0 20px;">{library_link}</td>
        <td align="center" style="padding: 0 20px;">{faq_link}</td>
        <td align="right" style="padding: 0 20px;">{subscription_link}</td>
      </tr>
    </table></center>
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
## Render the email version of the newsletter
##
def render_email_version( stories, gecko_unsub ):

    # GET STANDARD HEADER HTML SAME FOR ALL VERSIONS
    header_html = getHeaderAscii()

     
    # Properly encode the body for mailto link
    body_text = UNSUBSCRIBE_BODY.replace('\n', ' ').replace('"', '%22')  # Replace newlines with spaces and escape quotes
    body_encoded = urllib.parse.quote(body_text, safe='')  # Encode everything
    
    mailto_unsub = (
        f"<a style='color:chartreuse;font-weight:600;' href='mailto:{gecko_unsub}"
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
    top_html = f"<!DOCTYPE html><html><head><meta charset='UTF-8'><title>GEKKO'S BIRTHDAY</title><style>body,html{{background-color:black;color:white;margin:0;padding:0;font-family:'Tahoma',monospace;}}</style></head>"
    
    body_html = f"<body bgcolor='black' text='white' link='white' alink='white' style='background-color:black;color:white;margin:0;padding:0;font-family:'Verdana',monospace;'><BR><BR> \
    <table width='100%' border='0' cellspacing='0' cellpadding='0' bgcolor='black'><tr><td align='center'><table width='600' border='0' cellspacing='0' cellpadding='20' bgcolor='black'> \
    <tr><td bgcolor='black'><table width='100%' border='0' cellspacing='0' cellpadding='0' bgcolor='black' style='font-family:'Tahoma',monospace;'>{header_html}</table><BR><font style='font-family:Helvetica, sans-serif; letter-spacing:1.25px;'>{stories_html}</font>"
    
    footer_html = f"<hr color='white' size='1' style='border:none;border-top:1px solid white;margin:20px 0;'><div align='center' style='color:chartreuse;font-size:12px;text-align:center;margin-top:20px;'>&copy; {datetime.now().year} GEKKO'S BIRTHDAY Newsletter, produced by Scott Gross. All rights reserved.<BR>{mailto_subscribe}<BR></div></td></tr></table></td></tr></table><BR></body></html>"
    
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

    if not EMAIL_TARGET:
        logger.error("EMAIL_TARGET environment variable is not set")
        return 0
    


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
                Source=EMAIL_TARGET,
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
                        Source=EMAIL_TARGET,
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
    try:

        # 1. Get top stories and update their status (only update if not view-only)
        stories = get_and_update_stories(count=3)
        
        if not stories:
            return {
                'statusCode': 404,
                'body': json.dumps({"message": "No stories available for newsletter"}),
                'headers': {'Content-Type': 'application/json'}
            }
        

        # 2.  Get all subscribed users
        subscribers = get_subscribers()
        
        if not subscribers:
            return {
                'statusCode': 404,
                'body': json.dumps({"message": "No subscribed users found"}),
                'headers': {'Content-Type': 'application/json'}
            }
        
        
        # 3. Render the email version
        email_content = render_email_version(stories)



        # 4. Construct Subject
        # fancy algorithm for subject line
        title_dict = stories[0].get('title', {})
        title_str = title_dict.get('S', '') if isinstance(title_dict, dict) else str(title_dict)
        subject_snippet = extract_subject_snippet(title_str, num_words=3)
        fancy_subject = f"Gecko's Birthday - {subject_snippet}"

        # 5. Send emails
        sent_count = send_emails_to_subscribers(subscribers, email_content, fancy_subject)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                "message": f"Newsletter sent to {sent_count} recipients",
                "stories_count": len(stories),
                "recipients_count": len(subscribers),
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