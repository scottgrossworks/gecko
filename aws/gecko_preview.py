'''
## gecko_preview.py
## Handles /preview?email=someone@example.com
## Sends a personalized newsletter to a single email (for outreach/demos)

Requires and validates the email param
No mass send, no browser HTML output

takes optional params to create a user profile with
name,
zip,
interests,
status:null until they subscribe

SAVES new user profle to DDB
sk:email



'''
import re
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
TABLE_NAME = os.environ.get('DDB_NAME', 'gecko_db')
WEB_FUNCTION = os.environ.get('WEB_FUNCTION', 'gecko_web')
RENDER_FUNCTION = os.environ.get('RENDER_FUNCTION', 'gecko_render')
EMAIL_SOURCE = os.environ.get('EMAIL_SOURCE')
EMAIL_TARGET = os.environ.get('EMAIL_TARGET')
EMAIL_SUBJECT = os.environ.get('EMAIL_SUBJECT', "Gekko's Birthday * Preview Newsletter")


LIBRARY_LINK = os.environ.get('LIBRARY_LINK')
FAQ_LINK = os.environ.get('FAQ_LINK')

WIDTH = "'100%'"

SUBSCRIBE_BODY = (
    "I want news and insights at the intersection of business, technology, and culture. "
    "Like Bud Fox in Wall Street, I want to start my day with the information and opportunities that can change my life. "
    "Sign me up for the Gekko's Birthday Newsletter!"
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
                  GOOD MORNING
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
            <span style="display: inline-block; border: 2px solid white; color: red; background: black; font-size: 1.7em; font-weight: bold; border-radius: 5px; padding: 8px 20px; letter-spacing: 2px; font-family: Tahoma, Geneva, Verdana, sans-serif;">
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
## Render the email version of the newsletter
##
def render_email_version( stories, gecko_subscribe ):

    # GET STANDARD HEADER HTML SAME FOR ALL VERSIONS
    header_html = getHeaderAscii()

     
    # Properly encode the body for mailto link
    body_text = SUBSCRIBE_BODY.replace('\n', ' ').replace('"', '%22')  # Replace newlines with spaces and escape quotes
    body_encoded = urllib.parse.quote(body_text, safe='')  # Encode everything
    
    mailto_subscribe = (
        f"<a style='color:chartreuse;font-weight:600;' href='mailto:{gecko_subscribe}"
        f"?subject={urllib.parse.quote('Subscribe to Gekko\'s Birthday')}"
        f"&body={body_encoded}'>Subscribe</a>"
    )

    sub_html = render_links( mailto_subscribe )

    header_html += sub_html

    footer_html = f"<br>{mailto_subscribe}"


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







"""
    Fetch top stories from DynamoDB without updating their status
    
    Args:
        count (int): Number of stories to fetch
        
    Returns:
        list: List of story items in DynamoDB format
"""
def get_stories_without_update(count=3):
   
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



"""
Call the gecko_web.py lambda function
"""
def pass_to_web():
    try:
        payload = {}
        logger.info(f"Invoking {WEB_FUNCTION} Lambda")
        
        response = lambda_client.invoke(
            FunctionName=WEB_FUNCTION,
            InvocationType='RequestResponse',
            Payload=json.dumps(payload)
        )
        
        # Parse the response
        response_payload = json.loads(response['Payload'].read().decode())
        
        if response['StatusCode'] == 200:
            return response_payload
        else:
            logger.error(f"Error invoking {RENDER_EMAIL_FUNCTION} Lambda: {response_payload}")
            raise Exception(f"{RENDER_EMAIL_FUNCTION} Lambda returned status {response['StatusCode']}")
        
    except Exception as e:
        logger.error(f"Error invoking {RENDER_EMAIL_FUNCTION} Lambda: {str(e)}")
        raise



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




##
## PASS THROUGH TO gecko_render.py
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




"""
Send a single email to a recipient
"""
def send_single_email(email, email_content, subject):
   
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
        logger.info(f"Email sent to {email}")
        return True


    except ClientError as e:
        error_code = e.response['Error']['Code']
      
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
                        'Body': {'Html': {'Data': email_content}}
                    }
                )
                
            except Exception as retry_err:
                logger.error(f"Retry failed for {email}: {str(retry_err)}")
        else:
            logger.error(f"Error sending to {email}: {str(e)}")
    
    return True




##
##
##
def lambda_handler(event, context):
    try:

        # NO QUERY PARAMS --> straight to web
        if not event.get('queryStringParameters'):
            return pass_to_web()


        # Check if this is a direct email request
        single_recipient = None
        if isinstance(event, dict) and 'email' in event.get('queryStringParameters', {}):
            single_recipient = event['queryStringParameters']['email']
            logger.info(f"Sending preview to: {single_recipient}")
        
        else:
            # ERROR CONDITION
            logger.error("Missing email parameter")
            return {
                'statusCode': 400,
                'body': json.dumps({"message": "Missing email parameter"}),
                'headers': {'Content-Type': 'application/json'}
            }

        # 1. Get top stories and update their status (only update if not view-only)
        stories = get_stories_without_update(count=3)
        
        if not stories:
            return {
                'statusCode': 404,
                'body': json.dumps({"message": "No stories available for newsletter"}),
                'headers': {'Content-Type': 'application/json'}
            }
        

        # 2. Render the email version
        # EMAIL_TARGET is the gecko_subscribe email address
        email_content = render_email_version(stories, EMAIL_TARGET)


        # 3. Send email
        send_single_email(single_recipient, email_content, EMAIL_SUBJECT)

       
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                "message": f"Newsletter sent to {single_recipient}",
            }),
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

