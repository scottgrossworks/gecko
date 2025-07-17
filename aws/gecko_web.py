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


def getHeaderAscii():
    now = datetime.now(ZoneInfo("America/Los_Angeles"))
    time_str = now.strftime("%-I:%M %p").upper()
    date_str = now.strftime("%B %d, %Y - %A").upper()
    header_html = f"""
    <table width="100%" style="font-family: Tahoma, Geneva, Verdana, sans-serif; border-collapse: collapse; background: black;">
      <tr>
        <td style="padding: 2px 0 4px 0;">
          <div style="display: flex; justify-content: space-between; align-items: center;">
            <span style="color: chartreuse; font-size: 1.3em; font-weight: bold; letter-spacing: 2px;">GOOD MORNING</span>
            <span style="color: chartreuse; font-size: 1.3em; font-weight: bold; letter-spacing: 2px;">{time_str} PST</span>
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
          <span style="display: inline-block; border: 2px solid white; color: red; background: black; font-size: 1.7em; font-weight: bold; border-radius: 5px; padding: 8px 20px; letter-spacing: 2px;">
            GEKKO'S BIRTHDAY
          </span>
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
    
    library_link = f"<a href='{LIBRARY_LINK}' target='_blank' style='color: gold; font-weight: bold; text-decoration: none; font-size: 1em;'>MBA Library</a>"
    faq_link = f"<a href='{FAQ_LINK}' target='_blank' style='color: gold; font-weight: bold; text-decoration: none; font-size: 1em;'>FAQ</a>"

    header_html = "" 
    header_html += f"""
    <tr>
    <td style='color: white; padding-top: 12px; letter-spacing: 1.1px;'>
        <div style='display: flex; justify-content: space-between; align-items: center;'>
        <span><font style='color:gold; font-weight:600'>{library_link}</font></span>
        <span><font style='color:gold; font-weight:600'>{faq_link}</font></span>
        <span style='text-align: right;'>{subscription_link}</span>
        </div>
    </td>
    </tr>
    """
    return header_html




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

    web_link = f"<a href='{WEB_TARGET}' style='font-weight:600;text-decoration: none;'><font color='chartreuse'>Subscribe</font></a>"

    sub_html = render_links( web_link )

    header_html += sub_html

    stories_html = render_stories(stories)

    ## COMPOSE THE COMPLETE HTML
    ##
    top_html = f"<!DOCTYPE html><html><head><meta charset='UTF-8'><title>Gecko's Birthday Today</title><style>body,html{{background-color:black;color:white;margin:0;padding:0;font-family:'Tahoma',monospace;}}</style></head>"
    
    body_html = f"<body bgcolor='black' text='white' link='white' alink='white' style='background-color:black;color:white;margin:0;padding:0;font-family:'Verdana',monospace;'><BR><BR> \
    <table width='100%' border='0' cellspacing='0' cellpadding='0' bgcolor='black'><tr><td align='center'><table width='600' border='1' cellspacing='0' cellpadding='20' bordercolor='white' bgcolor='black' style='border:1px solid white;'> \
    <tr><td bgcolor='black'><table width='100%' border='0' cellspacing='0' cellpadding='0' bgcolor='black' style='font-family:'Tahoma',monospace;'>{header_html}</table><BR><font style='font-family:Helvetica, sans-serif; letter-spacing:1.25px;'>{stories_html}</font>"
    
    footer_html = f"<hr color='white' size='1' style='border:none;border-top:1px solid white;margin:20px 0;'><div align='center' style='color:chartreuse;font-size:12px;text-align:center;margin-top:30px;'>&copy; {datetime.now().year} GEKKO'S BIRTHDAY Newsletter. All rights reserved.<BR><BR>{web_link}<BR></div></td></tr></table></td></tr></table><BR></body></html>"
    
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





##
##
##
def lambda_handler(event, context):

    
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