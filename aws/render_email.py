##
## render_email.py
##
##

import os
import json
import boto3
from datetime import datetime
import re
from botocore.exceptions import ClientError
import logging
import urllib.parse

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize AWS clients
dynamodb = boto3.client('dynamodb')
TABLE_NAME = os.environ.get('DDB_name')



##
##
##
def format_date():
    """Format the current date for the newsletter"""
    return datetime.now().strftime("%B %d, %Y")



##  
##
##
def create_subscription_links(base_url, recipient_email, recipient_status):
    """Create HTML links for subscription actions based on recipient status"""
    encoded_email = urllib.parse.quote(recipient_email)
    subscribe_url = f"{base_url}?action=subscribe&email={encoded_email}"
    unsubscribe_url = f"{base_url}?action=unsubscribe&email={encoded_email}"
    manana_url = f"{base_url}?action=manana&email={encoded_email}"

    # FIRST-TIME VIEWER --> show Subscribe | Manana
    if not recipient_status or recipient_status == "new":
        return f'<a href="{subscribe_url}" style="font-weight:600;text-decoration: none;"><font color="chartreuse">Subscribe</font></a> | <a href="{manana_url}" style="text-decoration: none;"><font color="white">Mañana</font></a>'

    # ALREADY SUBSCRIBED --> show Unsubscribe
    elif recipient_status == "subscribed":
        return f'<a href="{unsubscribe_url}" style="text-decoration: none;"><font color="red">Unsubscribe</font></a>'
    
    # UNSUBSCRIBED --> (RE)Subscribe | Manana
    elif recipient_status == "unsubscribed":
        return f'<a href="{subscribe_url}" style="font-weight:600;text-decoration: none;"><font color="chartreuse">Subscribe</font></a> | <a href="{manana_url}" style="text-decoration: none;"><font color="white">Mañana</font></a>'

    # MANANA --> Subscribe | Unsubscribe | Mañana
    elif recipient_status == "manana":
        return f'<a href="{subscribe_url}" style="font-weight:600;text-decoration: none;"><font color="chartreuse">Subscribe</font></a> | <a href="{unsubscribe_url}" style="text-decoration: none;"><font color="red">Unsubscribe</font></a> | <a href="{manana_url}" style="text-decoration: none;"><font color="white">Mañana</font></a>'

    else:
        logger.error(f"Unknown recipient status: {recipient_status}")
        return ""


##
## start with full url
## return shortened, cleaned version in href
##
def getHref_fromUrl(url):
    # Strip protocol
    clean_url = re.sub(r'^https?://', '', url)

    # Strip www.
    clean_url = re.sub(r'^www\.', '', clean_url)

    # Truncate to 30 chars
    shortUrl = clean_url[:50] + "..." if len(clean_url) > 30 else clean_url

    # Render anchor tag
    href = f"<a href='{url}' style='color: #FFFFFF;text-decoration: none;'><font color='#FFFFFF'>{shortUrl}</font></a>"
    """Get the href from a URL"""
    return href




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
    <td style='color: #FFFFFF; padding-top: 10px;'>
        <div style='display: flex; justify-content: space-between; align-items: center;'>
        <span>{formatted_date}</span>
        <span style='text-align: right;'>{subscription_links}</span>
        </div>
    </td>
    </tr>
    """
    return header_html




##
##
##
##
def render_email_template(stories, api_endpoint="https://api.example.com/subscribe", recipient_email="subscriber@example.com", recipient_status="new"):
   
    # Get the header HTML
    header_html = getHeaderAscii()
    
    # Date and subscription links
    subscription_links = create_subscription_links(api_endpoint, recipient_email, recipient_status) 
    header_html += render_subs(subscription_links)
    
    #
    # ITERATE THROUGH STORIES
    # RENDER EACH ONE
    # 
    stories_html = ""
    for i, story in enumerate(stories):
    
        # EXTRACT CONTENT FROM STORY
        title = None
        url = None
        category = None
        summary = None
        # tags = []
        take = None
        
        
        if 'title' in story:
            title = story.get('title', {}).get('S', '')
        
        if 'url' in story:
            url = story.get('url', {}).get('S', '')
        
        if 'category' in story:
            category = story.get('category', {}).get('S', '')
        
        # TAGS ARE USEFUL FOR SEARCHING LATER -- NOT SHOWN AT THIS TIME
        # if 'tags' in story:
            # tags = story.get('tags', {}).get('SS', [])
        
        if 'summary' in story:
            summary = story.get('summary', {}).get('S', '')

        if 'take' in story:
            take = story.get('take', {}).get('S', '')
        
        if not (title and url and category and summary and take):
            logger.error("Malformed story pk: " + story['pk']['S'])
            continue    
        
        # create shortened, cleaned version of URL for href
        href = getHref_fromUrl(url)
        #
        # combine fields into story block HTML content
        #
        
        story_start = "<table width='100%' border='0' cellspacing='0' cellpadding='10' bgcolor='#000000' style='color:#FFFFFF;margin-bottom: 25px;'><tr><td bgcolor='#000000'>"

        # Split title from rest with a true break
        # Put category first, then title, then smaller URL
        story_content  = f"<div style='color: red; font-style: italic; font-size: 14px; margin-bottom: 6px;'><i>{category}</i></div>"
        story_content += f"<h2 style='color: #FFFFFF; margin: 0 0 4px 0;'>{title}</h2>"
        story_content += f"<div style='line-height: 1.2; font-size: 13px; margin: 0 0 14px 0;'>{href}</div>"
        story_content += f"<div style='line-height:1.4; margin-bottom: 8px;'><b style='color:chartreuse'>Story</b>:&nbsp;&nbsp;{summary}</div>"
        story_content += f"<div style='line-height:1.4; margin-bottom: 2px;'><b style='color:chartreuse'>Gecko's Take</b>:&nbsp;&nbsp;{take}</div>"

        story_end = "</i></p></td></tr></table>"
        stories_html += f"{story_start}{story_content}{story_end}"
    

    # Create the complete email HTML - 
    email_html = f"""<!DOCTYPE html><html><head><meta charset='UTF-8'><title>Gecko's Birthday Today</title><style>body,html{{background-color:#000000;color:#FFFFFF;margin:0;padding:0;font-family:'Courier New',monospace;}}</style></head><body bgcolor='#000000' text='#FFFFFF' link='#FFFFFF' vlink='#FFFFFF' alink='#FFFFFF' style='background-color:#000000;color:#FFFFFF;margin:0;padding:0;font-family:"Courier New",monospace;'><BR><BR><table width='100%' border='0' cellspacing='0' cellpadding='0' bgcolor='#000000'><tr><td align='center'><table width='600' border='1' cellspacing='0' cellpadding='20' bordercolor='#FFFFFF' bgcolor='#000000' style='border:1px solid #FFFFFF;'><tr><td bgcolor='#000000'><table width='100%' border='0' cellspacing='0' cellpadding='0' bgcolor='#000000' style='font-family:"Courier New",monospace;'>{header_html}</table><hr color='#FFFFFF' size='1' style='border:none;border-top:1px solid #FFFFFF;margin:20px 0;'>{stories_html}<hr color='#FFFFFF' size='1' style='border:none;border-top:1px solid #FFFFFF;margin:20px 0;'><div align='center' style='color:chartreuse;font-size:12px;text-align:center;margin-top:30px;'>&copy; {datetime.now().year} GECKO'S BIRTHDAY Newsletter. All rights reserved.<br><a href='{api_endpoint}?action=unsubscribe&email={urllib.parse.quote(recipient_email)}' style='color:red;text-decoration:none;'>Unsubscribe</a></div></td></tr></table></td></tr></table></body></html>"""
    
    return email_html

##
##
##
##
def lambda_handler(event, context):
    """Lambda handler for rendering email content"""
    try:
        # Get stories from event or fetch from DynamoDB
        stories = event.get('stories', [])
        if not stories:
            # If no stories provided, fetch from DynamoDB (implementation needed)
            logger.info("No stories provided in event, would fetch from DynamoDB here")
            pass
        

        API_ENDPOINT = os.environ.get('GECKO_API')
        if not API_ENDPOINT:
            raise ValueError("GECKO_API environment variable is not set")
        
        # Get recipient information
        recipient = event.get('recipient', {})
        recipient_email = recipient.get('email', 'subscriber@example.com')
        recipient_status = recipient.get('status', 'new')
        
        # Render email content
        email_content = render_email_template(stories, API_ENDPOINT, recipient_email, recipient_status)
        
        return {
            'statusCode': 200,
            'email_content': email_content,
            'stories_count': len(stories)
        }
        
    except Exception as e:
        logger.error(f"Error rendering email: {str(e)}")
        return {
            'statusCode': 500,
            'body': f"Error: {str(e)}"
        }

# EOF
# render_email