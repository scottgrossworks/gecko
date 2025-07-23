##
## ASYNC helper function that composes and sends X POSTS
## using tweepy
##
## MUST SET Environment Variables
##
##    postfix = validateEnviron("POSTFIX", 0)
##    API_key = validateEnviron("API_KEY", 1)
##    API_secret = validateEnviron("API_SECRET", 1)
##    access_token = validateEnviron("ACCESS_TOKEN", 1)
##    access_token_secret = validateEnviron("ACCESS_SECRET", 1)
##
##    WAIT_SEC = wait a random number of seconds from 1 to this number before posting
##
        
import os
import tweepy
import json

import random
import time

from decimal import Decimal
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)



    
 
#
# Use this to JSON encode the DYNAMODB output
#
#
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            long_str = str(obj)
            short_str = long_str.strip("Decimal()")
            return short_str
        return json.JSONEncoder.default(self, obj)
    
    



#
#
#
#
def handle_error( msg ):
  
    logger.error( msg )
    ret_obj = { "cd": 0,
                "err": msg
            }
    
    
    the_json = json.dumps(ret_obj, cls=DecimalEncoder)

    return the_json
    
    
    
    

#
#
#
#
def handle_success( msg ):
  
    ret_obj = { "cd": 1,
                "msg": msg
            }
    the_json = json.dumps(ret_obj, cls=DecimalEncoder)

    return the_json
    
    
    




#
#
def validateEnviron( var, required ):
    
    the_val = ""
    try:
        the_val = os.environ[var]
    except KeyError:
        if required:
            raise ValueError("Environment Variable not found: " + var)
        the_val = ""
        
    return the_val
    
    
    
    
    
    
# WORKS DIFFERENTLY than other lambdas
# not looking for queryStringParameters
# will throw ValueError
#
def validateParam( event, param, required ):
    
    value = ""
    
    if (param not in event):
            if required:
                raise ValueError("HTTP Request error.  No '" + param + "' event parameter")
    else:
        value = event[param] 
        
    return value





##
## POST a String message to X
##
def post_text( the_text ) :
    
    postfix = validateEnviron("POSTFIX", 0)
    final_text = the_text + postfix
    
    try:
        
        API_key = validateEnviron("API_KEY", 1)
        API_secret = validateEnviron("API_SECRET", 1)
        
        access_token = validateEnviron("ACCESS_TOKEN", 1)
        access_token_secret = validateEnviron("ACCESS_SECRET", 1)
        
 
        client = tweepy.Client(
            consumer_key=API_key, consumer_secret=API_secret,
            access_token=access_token, access_token_secret=access_token_secret
        )
        
        response = client.create_tweet( text = final_text )     
        
        return response
        
        
    except Exception as e:
        logger.error("Cannot post to X: " + final_text)
        logger.error( str( e ) )        
        raise
    
    



 
#
# this helper function contains code serving several different callers
# the function name is in the event request object
# other params will vary accordingly
#
#
def lambda_handler(event, context):
    
    function_name = ""
    try:
    
        WAIT_SEC = validateEnviron("WAIT_SEC", 1)
    
        # which function to call
        function_name = validateParam(event, "function", 1)
        match function_name:
        
            # post a single text String to X
            # 
            case "post_text":
   
                # generate random number
                int_wait_sec = random.randint(1, int(WAIT_SEC))
                # GOTO SLEEP
                time.sleep( int_wait_sec )
                
                the_post = validateParam(event, "x_post", 1)
                response = post_text( the_post )
            
                # SUCCESS
                return createHttpResponse( handle_success("Posted to X: " + the_post) )



            # ERROR condition
            #
            case _:
                raise Exception("Unknown function request: " + function_name)
        
        
    except Exception as e:
        str_err = str( e )
        logger.error(str_err)
        return createHttpResponse( handle_error( str_err ))
    




        
#
# Create the HTTP response object
#
#
def createHttpResponse( json_result ):
   
    response = {
            'statusCode': 200,
            'body': json_result,
            'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': '*',
            },
    }

    # logger.info("RETURNING RESPONSE")
    logger.info(response)
    
    
    return response

 
 

