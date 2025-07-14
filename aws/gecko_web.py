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
import logging


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

WEB_TARGET = os.environ.get('WEB_TARGET')  # for subscribe button
RENDER_FUNCTION = os.environ.get('RENDER_FUNCTION', 'gecko_render')


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
def render_web_version( stories, WEB_TARGET ):

    # Get the header HTML
    header_html = getHeaderAscii()

    web_link = f"<a href='{WEB_TARGET}' style='font-weight:600;text-decoration: none;'><font color='chartreuse'>Subscribe</font></a>"

    sub_html = render_subs( web_link )

    header_html += sub_html

    footer_html = f"<br>{web_link}"
    
    stories_html = render_stories(stories)

    ## COMPOSE THE COMPLETE HTML
    ##
    final_html = f"""<!DOCTYPE html><html><head><meta charset='UTF-8'><title>Gecko's Birthday Today</title><style>body,html{{background-color:#000000;color:#FFFFFF;margin:0;padding:0;font-family:'Courier New',monospace;}}</style></head><body bgcolor='#000000' text='white' link='white' alink='white' style='background-color:black;color:white;margin:0;padding:0;font-family:"Courier New",monospace;'><BR><BR><table width='100%' border='0' cellspacing='0' cellpadding='0' bgcolor='black'><tr><td align='center'><table width='600' border='1' cellspacing='0' cellpadding='20' bordercolor='white' bgcolor='black' style='border:1px solid white;'><tr><td bgcolor='black'><table width='100%' border='0' cellspacing='0' cellpadding='0' bgcolor='black' style='font-family:"Courier New",monospace;'>{header_html}</table><hr color='white' size='1' style='border:none;border-top:1px solid white;margin:20px 0;'>{stories_html}<hr color='white' size='1' style='border:none;border-top:1px solid white;margin:20px 0;'><div align='center' style='color:chartreuse;font-size:12px;text-align:center;margin-top:30px;'>&copy; {datetime.now().year} GECKO'S BIRTHDAY Newsletter. All rights reserved.{footer_html}</div></td></tr></table></td></tr></table></body></html>"""
    
    return final_html





##
##
##
def lambda_handler(event, context):
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
        web_content = render_web_version(stories, single_recipient)

       
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