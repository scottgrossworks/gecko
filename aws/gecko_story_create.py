# gecko_story_create
# lambda_function.py
#
# INJEST EMAIL FROM S3 gecko_story_inbox
# PARSE JSON FROM EMAIL
# STORE IN DDB gecko_db
# RETURN 200 SUCCESS!
#
#

import json
import boto3
from botocore.exceptions import ClientError
from email import policy
from email.parser import BytesParser
import logging
from datetime import datetime
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')
dynamodb = boto3.client('dynamodb')
# from config
TABLE_NAME = os.environ.get('DDB_name')

def lambda_handler(event, context):
    # logger.info(f"Raw event: {json.dumps(event)}")
    try:
        if (not TABLE_NAME):
            raise ValueError("DDB_NAME environment variable is not set")

        # Detailed event structure validation
        logger.error(f"Event structure: {json.dumps(event)}")  # Log the full event to see its structure
        
        if 'Records' not in event or not event['Records']:
            raise ValueError(f"Invalid event structure: missing or empty Records array: {json.dumps(event)}")
            
        record = event['Records'][0]
        
        if 's3' not in record:
            raise ValueError(f"Invalid record structure: missing s3 key: {json.dumps(record)}")
            
        if 'bucket' not in record['s3'] or 'name' not in record['s3']['bucket']:
            raise ValueError(f"Invalid S3 record: missing bucket name: {json.dumps(record['s3'])}")
            
        if 'object' not in record['s3'] or 'key' not in record['s3']['object']:
            raise ValueError(f"Invalid S3 record: missing object key: {json.dumps(record['s3'])}")
        
        # GET THE EMAIL FROM S3
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        
        logger.info(f"Processing S3 object: {bucket}/{key}")
        
        response = s3.get_object(Bucket=bucket, Key=key)
        raw_email = response['Body'].read()
        email_msg = BytesParser(policy=policy.default).parsebytes(raw_email)
        body_text = email_msg.get_body(preferencelist=("plain")).get_content().strip()
        
        if not body_text:
            raise ValueError("No text content found in email")

        #
        # logger.info(f"Raw body text: {body_text}")
        #
        start = body_text.find('{')
        end = body_text.rfind('}') + 1
        if start == -1 or end <= start:
            raise ValueError("No valid JSON object found")

        # CLEAN THE JSON FROM THE EMAIL BODY
        json_str = body_text[start:end].replace('\r\n', '').replace('\n', ' ').strip()
        
        #
        # logger.info(f"Extracted JSON string: {json_str}")
        #
        payload = json.loads(json_str)
        now = datetime.utcnow().isoformat() + 'Z'
        pk = 'story' if payload.get('summary') and payload.get('tags') else 'lead'
        
    

        # extract the data first
        # check for missing, required values
        # throw error if missing:
        #   title, url, category, summary, take
        #   

        title = payload.get('title', '')
        category = payload.get('category', 'Uncategorized')
        summary = payload.get('summary', '')
        take = payload.get('take', '')
        url = payload.get('url', '')

        if not (title and url and category and summary and take):
            raise ValueError("Missing required fields: title, url, category, summary, take")

        #
        # matches DDB schema for gecko_db
        #
        # 
        item = {
            'pk': {'S': pk},
            'sk': {'S': f'ts#{now}'},
            'title': {'S': title.strip()},
            'tags': {'L': [{'S': tag.strip()} for tag in payload.get('tags', [])]},
            'category': {'S': category.strip()},
            'summary': {'S': summary.strip()},
            'take': {'S': take.strip()},
            'url': {'S': url.strip()},
            'date_created': {'S': now},
            'status': {'S': 'queued'},  # default to queued
            'published_date': {'NULL': True}
        }
        
        # PUT ITEM IN DDB
        dynamodb.put_item(TableName=TABLE_NAME, Item=item)
        

        logger.info(f"Story added to '{pk}' queue: {payload.get('title', 'Untitled')}")
        
        #
        # RETURN 200 SUCCESS!
        #
        return {'statusCode': 200, 'body': f"Story added to '{pk}' queue: {payload.get('title', 'Untitled')}"}
    
    
    #
    # ERROR HANDLING
    #
    except ClientError as e:
        error_msg = f"AWS Error: {str(e)}"
        logger.error(error_msg)
        return {'statusCode': 500, 'body': error_msg}
    except ValueError as e:
        error_msg = f"Validation Error: {str(e)}"
        logger.error(error_msg)
        return {'statusCode': 400, 'body': error_msg}
    except json.JSONDecodeError as e:
        error_msg = f"JSON Error: {str(e)} at position {e.pos}"
        logger.error(error_msg)
        return {'statusCode': 400, 'body': error_msg}
    except Exception as e:
        error_msg = f"Unexpected Error: {str(e)}"
        logger.error(error_msg)
        logger.error(f"Event that caused error: {json.dumps(event)}")
        return {'statusCode': 500, 'body': error_msg}


        ## EOF story create

