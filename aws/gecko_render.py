##
## gecko_render --> lambda_function.py
## just does the main story retrieval and rendering
##

import os
import boto3
import re
import logging

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
dynamodb = boto3.client('dynamodb')
TABLE_NAME = os.environ.get('DDB_NAME')





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
##
## ITERATE THROUGH STORIES
## RENDER EACH ONE
##
def render_stories( stories ):
  
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
        
        story_start = "<table width='100%' border='0' cellspacing='0' cellpadding='10' bgcolor='black' style='color:white;margin-bottom: 20px;'><tr><td bgcolor='black'>"

        # Split title from rest with a true break
        # Put category first, then title, then smaller URL
        story_content  = f"<div style='color: red; font-style: italic; font-size: 15px; margin-bottom: 6px;'><i>{category}</i></div>"
        story_content += f"<h2 style='color: white; margin: 0 0 4px 0;'>{title}</h2>"
        story_content += f"<div style='line-height: 1.2; font-size: 13px; margin: 0 0 14px 0;'>{href}</div>"
        story_content += f"<div style='line-height:1.4; margin-bottom: 8px;'><b style='color:chartreuse'>Story</b>:&nbsp;&nbsp;{summary}</div>"
        story_content += f"<div style='line-height:1.4; margin-bottom: 2px;'><b style='color:chartreuse;letter-spacing:1.25px;'>Gekko's Take</b>:&nbsp;&nbsp;{take}</div>"

        story_end = "</i></p></td></tr></table>"
        stories_html += f"{story_start}{story_content}{story_end}"
    
    return stories_html




##
##
##
##
def lambda_handler(event, context):
    
    try:
        # Get stories from event or fetch from DynamoDB
        stories = event.get('stories', [])
        if not stories:
            logger.info("No stories provided in event, would fetch from DynamoDB here")
            
        html_content = render_stories(stories)

        return {
            'statusCode': 200,
            'html_content': html_content,
            'stories_count': len(stories),
        }

    except Exception as e:
        logger.error(f"Error rendering stories: {str(e)}")
        return {
            'statusCode': 500,
            'body': f"Error: {str(e)}"
        }

# EOF
# gecko_render.py