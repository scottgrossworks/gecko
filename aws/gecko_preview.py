
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
WEB_LAMBDA = os.environ.get('WEB_LAMBDA', 'gecko_web')
RENDER_FUNCTION = os.environ.get('RENDER_FUNCTION', 'gecko_render')
EMAIL_SOURCE = os.environ.get('EMAIL_SOURCE', 'gecko@scottgross.works')


SUBSCRIBE_BODY = (
    "I want news and insights at the intersection of business, technology, and culture. "
    "Like Bud Fox in Wall Street, I want to start my day with the information and opportunities that can change my life. "
    "Sign me up for Gecko's Birthday and let's get started!"
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
## Render the email version of the newsletter
##
def render_email_version( stories, EMAIL_TARGET ):

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
        logger.info(f"Invoking {WEB_LAMBDA} Lambda")
        
        response = lambda_client.invoke(
            FunctionName=WEB_LAMBDA,
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
            logger.info(f"Single recipient mode for: {single_recipient}")
        
        else:
            # ERROR CONDITION
            logger.error("Missing email parameter")
            return {
                'statusCode': 400,
                'body': json.dumps({"message": "Missing email parameter"}),
                'headers': {'Content-Type': 'application/json'}
            }

        # single_recipient contains target email_address 
        # 1. Get top stories and update their status (only update if not view-only)
        stories = get_stories_without_update(count=3)
        
        if not stories:
            return {
                'statusCode': 404,
                'body': json.dumps({"message": "No stories available for newsletter"}),
                'headers': {'Content-Type': 'application/json'}
            }
        

        # 2. Render the email version
        email_content = render_email_version(stories, single_recipient)


        # 3. Construct Subject
        subject = "Gecko's Birthday- Preview Newsletter"

        # 4. Send email
        send_single_email(single_recipient, email_content, subject)

       
        
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


