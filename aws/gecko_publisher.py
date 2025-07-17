'''
THIS IS THE lambda_function.py code

Handles /publish

Sends newsletter to all subscribers in DynamoDB (production run)

No preview, no web rendering, no single-email outreach

'''

import os
import json
import boto3
import time
import logging
from datetime import datetime
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

EMAIL_TARGET = os.environ.get('EMAIL_TARGET') # for subscribe button

LIBRARY_LINK = os.environ.get('LIBRARY_LINK')
FAQ_LINK = os.environ.get('FAQ_LINK')



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
 # Create the header HTML directly with proper colors and spacing
    header_html = ""
    
    # Top border - chartreuse green
    header_html += "<tr style='padding-left:20px;'>"
    header_html += "<td style='color: chartreuse;'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"
    header_html += "╔════════════════════════════════════════════╗</td></tr>"
    
    # GECKO'S BIRTHDAY line - with red asterisk
    header_html += "<tr style='padding-left:20px;'><td>"
    header_html += "&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"
    header_html += "<span style='color: chartreuse;'>{&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"
    header_html += "<font color='red' style='font-weight:600;letter-spacing:0.1em;'>GECKO'S BIRTHDAY</font></span>"
    header_html += "<span>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</span>"
    header_html += "<span style='color: red;'>*</span>"
    header_html += "<span style='color: chartreuse;'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;.<font style='letter-spacing:0.1em;'>&nbsp;&nbsp;║</font></span>"
    header_html += "</td></tr>"
    
    # News • Markets • AI line - with white text and carets
    header_html += "<tr style='padding-left:20px;'><td>"
    header_html += "&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"
    header_html += "<span style='color: chartreuse;'>{&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</span>"
    header_html += "<span style='color: #FFFFFF;'>News • Markets • Ai</span>"
    header_html += "<span style='color: #FFFFFF;'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</span>"
    header_html += "<span style='color: #FFFFFF;'>^^^^^^^^^</span>"
    header_html += "<span style='color: chartreuse;'>══╝</span>"
    header_html += "</td></tr>"
    
    # By Scott Gross line - with white carets and red dashes
    header_html += "<tr style='padding-left:20px;'><td>"
    header_html += "&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"
    header_html += "<span style='color: chartreuse;'>{&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;By Scott Gross</span>"
    header_html += "<span style='color: chartreuse;'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</span>"
    header_html += "<span style='color: #FFFFFF;'>^^</span>"
    header_html += "<span style='color: red;'>----</span>"
    header_html += "<span style='color: #FFFFFF;'>^^^</span>"
    header_html += "<span style='color: chartreuse;'>══╗</span>"
    header_html += "</td></tr>"
    
    # Bottom border - chartreuse green
    header_html += "<tr style='padding-left:20px'>"
    header_html += "<td style='color: chartreuse;'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"
    header_html += "╚════════════════════════════════════════════╝</td></tr>"

    return header_html


##
##
##
def render_subs( subscription_links ):

    formatted_date = format_date()
    header_html = "" 
    header_html += f"""
    <tr>
    <td style='color: white; padding-top: 10px;'>
        <div style='display: flex; justify-content: space-between; align-items: center;'>
        <span><font style='color:gold; font-weight:600'>{formatted_date}</font></span>
        <span style='text-align: right;'>{subscription_links}</span>
        </div>
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
def render_email_version( stories ):

    # GET STANDARD HEADER HTML SAME FOR ALL VERSIONS
    header_html = getHeaderAscii()

     
    mailto_subscribe = (
        f"mailto:{EMAIL_TARGET}"
        f"?subject=Subscribe%20me%20to%20Gecko's%20Birthday"
        f"&body={urllib.parse.quote(SUBSCRIBE_BODY)}"
    )

    sub_html = render_subs( mailto_subscribe )

    header_html += sub_html

    footer_html = f"<br><a href='{mailto_subscribe}' style='color:red;text-decoration:none;'>Subscribe</a>"


    # RENDER THE STORIES AS HTML STRING
    stories_html = render_stories(stories)


    ## COMPOSE THE COMPLETE HTML
    ##
    email_html = f"""<!DOCTYPE html><html><head><meta charset='UTF-8'><title>Gecko's Birthday Today</title><style>body,html{{background-color:#000000;color:#FFFFFF;margin:0;padding:0;font-family:'Courier New',monospace;}}</style></head><body bgcolor='#000000' text='white' link='white' alink='white' style='background-color:black;color:white;margin:0;padding:0;font-family:"Courier New",monospace;'><BR><BR><table width='100%' border='0' cellspacing='0' cellpadding='0' bgcolor='black'><tr><td align='center'><table width='600' border='1' cellspacing='0' cellpadding='20' bordercolor='white' bgcolor='black' style='border:1px solid white;'><tr><td bgcolor='black'><table width='100%' border='0' cellspacing='0' cellpadding='0' bgcolor='black' style='font-family:"Courier New",monospace;'>{header_html}</table><hr color='white' size='1' style='border:none;border-top:1px solid white;margin:20px 0;'>{stories_html}<hr color='white' size='1' style='border:none;border-top:1px solid white;margin:20px 0;'><div align='center' style='color:chartreuse;font-size:12px;text-align:center;margin-top:30px;'>&copy; {datetime.now().year} GECKO'S BIRTHDAY Newsletter. All rights reserved.{footer_html}</div></td></tr></table></td></tr></table></body></html>"""


    return email_html




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
    filtered = [w for w in words if w.lower() not in stopwords or words.index(w) > 0]
    # Take the first num_words after skipping stopwords
    snippet = " ".join(filtered[:num_words])
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