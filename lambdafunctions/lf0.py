import json
import datetime
import boto3
import uuid

def lambda_handler(event, context):
    print(event)
    messageList = event['messages']
    final_response = "Something went wrong, please try again."
    session_id = str(uuid.uuid4())
    
    if messageList is not None or len(messageList) > 0:
        prompt = messageList[0]['unstructured']['text']
        # lex = boto3.client('lexv2-runtime')
        # moving away from lexv2 since its response does not give all slots at once
        lex = boto3.client('lex-runtime')
        lex_response = lex.post_text(botName='DiningConciergeBot', botAlias='dev_bot', userId='test', inputText=prompt)
        
        if lex_response is not None and len(lex_response) > 0:
            final_response = lex_response

    response = {
        'messages': [
            {
                "type":"unstructured",
                "unstructured": {
                    "id":"1",
                    "text": final_response,
                    "timestamp": str(datetime.datetime.now().timestamp())
                }
            }
        ]
    }
    
    return {
        'statusCode': 200,
        'headers': {
            # 'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': 'https://diningconciergebot.s3.amazonaws.com',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        },
        'body': response
    }
