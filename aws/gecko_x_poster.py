##
## GECKO X POSTER Lambda Function
##
## Called by gecko_story_create after a new story is added to DynamoDB
## Composes a tweet with URL + take and posts to X via async_x_handler
##
## Environment Variables Required:
##   X_HANDLER_FUNCTION_NAME - Name of the async_x_handler Lambda function
##   MAX_TWEET_LENGTH - Maximum tweet length (default: 280)
##

import json
import boto3
import logging
from decimal import Decimal

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize Lambda client for calling async_x_handler
lambda_client = boto3.client('lambda')

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        return super(DecimalEncoder, self).default(obj)

def validate_environ(var, required=True):
    """Get environment variable with validation"""
    import os
    try:
        return os.environ[var]
    except KeyError:
        if required:
            raise ValueError(f"Environment Variable not found: {var}")
        return ""

def compose_tweet(story_data):
    """
    Compose a tweet from story data
    Format: [url] [take]
    """
    MAX_TWEET_LENGTH = int(validate_environ("MAX_TWEET_LENGTH", False) or "280")
    
    title = story_data.get('title', '')
    take = story_data.get('take', '')
    url = story_data.get('url', '')
    
    if not take or not url:
        raise ValueError("Story missing required fields: take and url")
    
    # Start with url + space + take
    base_tweet = f"{url} {take}"
    
    # If too long, truncate take
    if len(base_tweet) > MAX_TWEET_LENGTH:
        # Reserve space for URL + space + ellipsis
        url_space = len(url) + 4  # space + "..."
        max_take_length = MAX_TWEET_LENGTH - url_space
        
        if max_take_length > 10:  # Ensure minimum take length
            truncated_take = take[:max_take_length] + "..."
            base_tweet = f"{url} {truncated_take}"
        else:
            # If take is too long, just use URL
            base_tweet = url
    
    logger.info(f"Composed tweet ({len(base_tweet)} chars): {base_tweet}")
    return base_tweet

def call_x_handler(tweet_text):
    """
    Call the async_x_handler Lambda function to post tweet
    """
    function_name = validate_environ("X_HANDLER_FUNCTION_NAME")
    
    payload = {
        "function": "post_text",
        "x_post": tweet_text
    }
    
    try:
        response = lambda_client.invoke(
            FunctionName=function_name,
            InvocationType='Event',  # Async invocation
            Payload=json.dumps(payload)
        )
        
        logger.info(f"Successfully invoked {function_name}")
        return {
            "success": True,
            "statusCode": response['StatusCode'],
            "message": f"Tweet queued for posting: {tweet_text[:50]}..."
        }
        
    except Exception as e:
        logger.error(f"Failed to invoke {function_name}: {str(e)}")
        raise

def lambda_handler(event, context):
    """
    Main Lambda handler
    Expected event format from gecko_story_create:
    {
        "story_data": {
            "title": "...",
            "take": "...", 
            "url": "...",
            "category": "...",
            "summary": "..."
        }
    }
    """
    
    try:
        logger.info(f"Received event: {json.dumps(event, cls=DecimalEncoder)}")
        
        # Extract story data from event - handle both wrapped and unwrapped formats
        story_data = event.get('story_data')
        if not story_data:
            # Check if event contains story fields directly (for testing)
            if event.get('title') and event.get('take') and event.get('url'):
                story_data = event  # Use event directly as story data
            else:
                raise ValueError("No story_data found in event and event doesn't contain required story fields (title, take, url)")
        
        # Compose tweet
        tweet_text = compose_tweet(story_data)
        
        # Post to X via async_x_handler
        result = call_x_handler(tweet_text)
        
        # Return success response
        response = {
            'statusCode': 200,
            'body': json.dumps({
                "success": True,
                "message": "Story posted to X successfully",
                "tweet": tweet_text,
                "result": result
            }, cls=DecimalEncoder),
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            }
        }
        
        logger.info("X posting completed successfully")
        return response
        
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Error in gecko_x_poster: {error_msg}")
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                "success": False,
                "error": error_msg
            }, cls=DecimalEncoder),
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            }
        }

# EOF gecko_x_poster.py
