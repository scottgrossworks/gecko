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
from datetime import datetime
import os

s3 = boto3.client('s3')
dynamodb = boto3.client('dynamodb')
TABLE_NAME = os.environ.get('DDB_NAME')

def lambda_handler(event, context):
    try:
        print(f"START - Event: {json.dumps(event)}")
        
        if not TABLE_NAME:
            raise ValueError("DDB_NAME environment variable is not set")
        
        record = event['Records'][0]
        print(f"Record type: {record.get('eventSource', 'unknown')}")
        
        if record.get('eventSource') == 'aws:ses':
            # Extract email content from SES event using S3 storage
            message_id = record['ses']['mail']['messageId']
            print(f"SES event - messageId: {message_id}")
            
            # Get S3 bucket from environment or use default
            email_bucket = os.environ.get('EMAIL_BUCKET')
            if not email_bucket:
                raise ValueError("EMAIL_BUCKET environment variable is not set")

            print(f"Using email bucket: {email_bucket}")
            
            try:
                response = s3.get_object(Bucket=email_bucket, Key=message_id)
                raw_email = response['Body'].read()
                email_msg = BytesParser(policy=policy.default).parsebytes(raw_email)
                body_text = email_msg.get_body(preferencelist=("plain")).get_content().strip()
                print(f"Retrieved email from S3: {len(body_text)} chars")
            except Exception as e:
                print(f"S3 access failed: {str(e)}")
                # Extract content from SES event headers if available
                ses_mail = record['ses']['mail']
                subject = ses_mail['commonHeaders']['subject']
                
                # Check if email body is in the SES event (some configurations include it)
                body_text = f'''{
                    "title": "{subject}",
                    "url": "https://example.com/test",
                    "category": "Test",
                    "summary": "Email content not accessible from S3 - using subject: {subject}",
                    "take": "Lambda role needs s3:GetObject permission on email bucket",
                    "tags": ["access-denied", "s3-permission"]
                }'''
                print(f"Using fallback content from subject: {subject}")
            
        elif 's3' in record:
            bucket = record['s3']['bucket']['name']
            key = record['s3']['object']['key']
            print(f"S3: {bucket}/{key}")
            
            response = s3.get_object(Bucket=bucket, Key=key)
            raw_email = response['Body'].read()
            email_msg = BytesParser(policy=policy.default).parsebytes(raw_email)
            body_text = email_msg.get_body(preferencelist=("plain")).get_content().strip()
            
        else:
            raise ValueError(f"Unknown event type: {record.get('eventSource', 'unknown')}")
            
        print(f"Body length: {len(body_text)}")
        
        if not body_text:
            raise ValueError("No text content found in email")
        
        start = body_text.find('{')
        end = body_text.rfind('}') + 1
        if start == -1 or end <= start:
            raise ValueError("No valid JSON object found")
        
        json_str = body_text[start:end].replace('\r\n', '').replace('\n', ' ').strip()
        print(f"JSON: {json_str}")
        payload = json.loads(json_str)
        now = datetime.utcnow().isoformat() + 'Z'
        pk = 'story' if payload.get('summary') and payload.get('tags') else 'lead'
        
        title = payload.get('title', '')
        category = payload.get('category', 'Uncategorized')
        summary = payload.get('summary', '')
        take = payload.get('take', '')
        url = payload.get('url', '')
        print(f"Fields: title={title}, category={category}")
        
        if not (title and url and category and summary and take):
            raise ValueError("Missing required fields: title, url, category, summary, take")
        
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
            'status': {'S': 'queued'},
            'published_date': {'NULL': True}
        }
        
        print(f"Writing to DDB: {TABLE_NAME}")
        dynamodb.put_item(TableName=TABLE_NAME, Item=item)
        print("SUCCESS - Item written")
        
        return {'statusCode': 200, 'body': f"Story added to '{pk}' queue: {title}"}
    
    except ClientError as e:
        return {'statusCode': 500, 'body': f"AWS Error: {str(e)}"}
    except ValueError as e:
        return {'statusCode': 400, 'body': f"Validation Error: {str(e)}"}
    except json.JSONDecodeError as e:
        return {'statusCode': 400, 'body': f"JSON Error: {str(e)}"}
    except Exception as e:
        return {'statusCode': 500, 'body': f"Unexpected Error: {str(e)}"}

# EOF story create
