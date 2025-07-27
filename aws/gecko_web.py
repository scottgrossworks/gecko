## gecko_web.py
##
'''
Handles /web (or /preview with no params if you prefer)

Returns a generic HTML preview for browser/testing

No user lookup, no SES sending

Subscribe button links to landing page (not mailto:)

'''

import os
import json
from datetime import datetime
try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except ImportError:
    from backports.zoneinfo import ZoneInfo  # For local dev if needed

import logging

import boto3
from botocore.exceptions import ClientError


# Initialize AWS clients
dynamodb = boto3.client('dynamodb')
ses = boto3.client('ses')
lambda_client = boto3.client('lambda')


# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)



# Environment variables
TABLE_NAME = os.environ.get('DDB_NAME', 'gecko_db')
WEB_TARGET = os.environ.get('WEB_TARGET')  # for subscribe button
RENDER_FUNCTION = os.environ.get('RENDER_FUNCTION', 'gecko_render')

LIBRARY_LINK = os.environ.get('LIBRARY_LINK')
FAQ_LINK = os.environ.get('FAQ_LINK')


##
##
##
def format_date():
    """Format the current date for the newsletter"""
    return datetime.now().strftime("%B %d, %Y")



##
## Helper function -- return the ASCII header
##


def get_greeting(hour):
    if 5 <= hour < 12:
        return "GOOD MORNING"
    elif 12 <= hour < 17:
        return "GOOD AFTERNOON"
    else:
        return "GOOD EVENING"

def getHeaderAscii():
    now = datetime.now(ZoneInfo("America/Los_Angeles"))
    time_str = now.strftime("%-I:%M %p").upper()
    date_str = now.strftime("%B %d, %Y - %A").upper()
    greeting = get_greeting(now.hour)
    header_html = f"""
    <table width="100%" style="font-family: Tahoma, Geneva, Verdana, sans-serif; border-collapse: collapse; background: black;">
      <tr>
        <td style="padding: 2px 0 4px 0;">
          <div style="display: flex; justify-content: space-between; align-items: center;">
            <span style="width: 60px; display: flex; align-items: center;">
              <a href="http://scottgross.works" target="_blank" rel="noopener">
              <img src="https://s3.us-west-2.amazonaws.com/scottgross.works/SGW_favicon.ico" style="height: 32px; width: 32px;" alt="SGW">
                </a>
            </span>
            <span style="color: chartreuse; font-size: 1.3em; font-weight: bold; letter-spacing: 2px;">{greeting}!</span>
            <span style="color: chartreuse; font-size: 1.3em; font-weight: bold; letter-spacing: 2px;">{time_str}</span>
          </div>
        </td>
      </tr>
      <tr>
        <td style="text-align: center; color: white; font-size: 1.1em; font-weight:600; padding: 10px; letter-spacing: 1.3px;">
          {date_str}
        </td>
      </tr>
      <tr>
        <td style="text-align: center; padding: 12px 0 10px 0;">
          <div style="display: inline-block; border: 2px solid white; border-radius: 11px; padding: 3px; background: #111111;">
            <span style="display: inline-block; border: 2px solid white; color: red; background: #111111; font-size: 2em; font-weight: bold; border-radius: 7px; padding: 10px 25px; letter-spacing: 2.5px; box-shadow: 0 0 12px #000a;">
              GEKKO'S BIRTHDAY
            </span>
          </div>
        </td>
      </tr>
    </table>
    """
    return header_html



## Removed render_links function as it's now handled inline




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





##
## Render the web version of the newsletter
##
def render_web_version( stories ):

    # Get the header HTML
    header_html = getHeaderAscii()

    web_link = f"<a href='{WEB_TARGET}' style='font-weight:600;font-size:1.2em; text-decoration:none'><font color='chartreuse'>Subscribe</font></a>"

    ## Separate links HTML outside the header table
    links_html = f"""
    <div style='width:100%; padding: 20px 0; display:flex; justify-content:center; align-items:center; gap: 1.5em;'>
        <span><a href='{LIBRARY_LINK}' target='_blank' style='color: gold; font-weight: bold; text-decoration: none; font-size: 1.1em;'>MBA Links</a></span>
        <span style='color: chartreuse; font-weight: bold; font-size: 1.1em;'>|</span>
        <span><a href='{FAQ_LINK}' target='_blank' style='color: white; font-weight: bold; text-decoration: none; font-size: 1.1em;'>FAQ</a></span>
        <span style='color: chartreuse; font-weight: bold; font-size: 1.1em;'>|</span>
        <span>{web_link}</span>
    </div>
    """

    stories_html = render_stories(stories)

    ## COMPOSE THE COMPLETE HTML
    ##
    top_html = f"<!DOCTYPE html><html><head><meta charset='UTF-8'><title>Gekko's Birthday</title><link rel='icon' type='image/x-icon' href='https://s3.us-west-2.amazonaws.com/scottgross.works/SGW_favicon.ico'><style>body,html{{background-color:black;color:white;margin:0;padding:0;font-family:'Tahoma',monospace;}}</style></head>"
    
    body_html = f"<body bgcolor='black' text='white' link='white' alink='white' style='background-color:black;color:white;margin:0;padding:0;font-family:'Verdana',monospace;'><BR><BR> \
    <table width='100%' border='0' cellspacing='0' cellpadding='0' bgcolor='black'><tr><td align='center'><table width='600' border='1' cellspacing='0' cellpadding='20' bordercolor='white' bgcolor='black' style='border:1px solid white;'> \
    <tr><td bgcolor='black'><table width='100%' border='0' cellspacing='0' cellpadding='0' bgcolor='black' style='font-family:'Tahoma',monospace;'>{header_html}</table></td></tr></table></td></tr></table> \
    <table width='600' border='0' cellspacing='0' cellpadding='0' style='margin:0 auto;'><tr><td>{links_html}</td></tr></table> \
    <table width='100%' border='0' cellspacing='0' cellpadding='0' bgcolor='black'><tr><td align='center'><table width='600' border='1' cellspacing='0' cellpadding='20' bordercolor='white' bgcolor='black' style='border:1px solid white;'> \
    <tr><td bgcolor='black'><font style='font-family:Helvetica, sans-serif; letter-spacing:1.25px;'>{stories_html}</font>"
    
    footer_html = f"<hr color='white' size='1' style='border:none;border-top:1px solid white;margin:20px 0;'><div align='center' style='color:chartreuse;font-size:12px;text-align:center;margin-top:30px;'>&copy; {datetime.now().year} GEKKO'S BIRTHDAY Newsletter, produced by \
        <a href='http://scottgross.works'>Scott Gross</a>. All rights reserved.<BR><BR>{web_link}<BR></div></td></tr></table></td></tr></table><BR></body></html>"
    
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
##
##
def lambda_handler(event, context):
    # Log all incoming requests for debugging
    logger.info(f"Incoming web request - Method: {event.get('httpMethod')}, Path: {event.get('path')}, QueryParams: {event.get('queryStringParameters')}")
    
    # Handle favicon requests
    if event.get('path') == '/favicon.ico':
        return {
            'statusCode': 301,  # Permanent redirect
            'headers': {
                'Location': 'https://s3.us-west-2.amazonaws.com/scottgross.works/SGW_favicon.ico',
                'Cache-Control': 'public, max-age=86400'  # Cache for 24 hours
            },
            'body': ''
        }
    
    logger.info(f"Generating web version....")
            
    try:

        # 1. Get top stories and update their status (only update if not view-only)
        stories = get_stories_without_update(count=3)
        
        if not stories:
            return {
                'statusCode': 404,
                'body': json.dumps({"message": "No stories available for newsletter"}),
                'headers': {'Content-Type': 'application/json'}
            }
       
       
        # 2. Render the web version
        web_content = render_web_version(stories)

       
        return {
            'statusCode': 200,
            'body': web_content,  # this is your HTML string
            'headers': {
                'Content-Type': 'text/html'
            }
        }
        
    except Exception as e:
        logger.error(f"Error in gecko_preview: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({"message": f"Error: {str(e)}"}),
            'headers': {'Content-Type': 'application/json'}
        }



# EOF gecko_web.py