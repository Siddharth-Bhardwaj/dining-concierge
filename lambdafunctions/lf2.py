import json
import boto3
import random
import datetime

# import os
# import sys
# import subprocess
# subprocess.call('pip3 install requests -t /tmp/ --no-cache-dir'.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
# subprocess.call('pip3 install elasticsearch==7.13.4 -t /tmp/ --no-cache-dir'.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
# sys.path.insert(1, '/tmp/')

import requests
from elasticsearch import Elasticsearch, RequestsHttpConnection
from botocore.exceptions import ClientError

def getMessages():
    sqsClient = boto3.client('sqs')
    receiveResponse = sqsClient.receive_message(
        QueueUrl='https://sqs.us-east-1.amazonaws.com/550673492243/diningConciergeQueue',
        AttributeNames=[
            'date', 'cuisine', 'location', 'num_people', 'email', 'time'
        ],
        MaxNumberOfMessages=10,
        MessageAttributeNames=[
            'All'
        ],
        VisibilityTimeout=20,
        WaitTimeSeconds=20
    )
    
    print(receiveResponse)

    if 'Messages' in receiveResponse.keys():
        messages = receiveResponse['Messages']
        for message in messages:
            receiptHandle = message['ReceiptHandle']
            sqsClient.delete_message(QueueUrl='https://sqs.us-east-1.amazonaws.com/550673492243/diningConciergeQueue', ReceiptHandle=receiptHandle)
            print('deleted ' + receiptHandle)
        return messages
    
    return []
    
def queryElasticSearch(cuisine, esClient):
    query = {"query": {"match": {"cuisine": cuisine}}}
    result = esClient.search(index="restaurant", body=query, size=50)
    
    randomList = list(range(50))
    random.shuffle(randomList)
    businessIds = []
    for i in range(0, 5):
        if result['hits']['hits'][randomList[i]]['_source']['id'] is not None:
            businessIds.append(result['hits']['hits'][randomList[i]]['_source']['id'])
            
    return businessIds
    
def queryDynamoDB(businessIds, dbTable):
    restaurants = []
    for businessId in businessIds:
        restaurants.append(dbTable.get_item(Key = {'businessId': businessId}).get("Item"))
    return restaurants
    
def constructEmailMessage(cuisine, time, date, people, restaurants):
    email = "Hi, this is your dining concierge! \n" + "Here are my " + cuisine + " restaurant suggestions for " + people + " people, for " + date + " at " + time + ":\n\n"
    i = 1
    for restaurant in restaurants:
        email += "[" + str(i) + "] " + restaurant["name"] + " , located at " + restaurant["address"] + "\n\n"
        i += 1
    
    email += "\nEnjoy your meal!"
        
    return email

def lambda_handler(event, context):
    print('triggered lf2 at ' + str(datetime.datetime.now()))
    messages = getMessages()

    esClient = Elasticsearch(
        hosts=["https://search-dining-concierge-sfd4ecm7m3s4behekq7gyck64e.aos.us-east-1.on.aws:443"],
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection,
        http_auth=('dining-concierge', 'Diningconcierg@3')
        )
    dbClient = boto3.resource('dynamodb')
    dbTable = dbClient.Table('yelp-restaurants')
    sesClient = boto3.client('ses')

        
    for message in messages:
        cuisine = message["MessageAttributes"]["cuisine"]["StringValue"]
        email = message["MessageAttributes"]["email"]["StringValue"]
        time = message["MessageAttributes"]["time"]["StringValue"]
        date = message["MessageAttributes"]["date"]["StringValue"]
        people = message["MessageAttributes"]["num_people"]["StringValue"]
        
        businessIds = queryElasticSearch(cuisine, esClient)
        restaurants = queryDynamoDB(businessIds, dbTable)
        
        emailMessage = constructEmailMessage(cuisine, time, date, people, restaurants)

        try:
            response = sesClient.send_email(
                Destination={
                    'ToAddresses': [
                        email,
                    ],
                },
                Message={
                    'Body': {
                        'Text': {
                            'Charset': 'UTF-8',
                            'Data': emailMessage,
                        },
                    },
                    'Subject': {
                        'Charset': 'UTF-8',
                        'Data': 'Your Dining Suggestions',
                    },
                },
                Source='siddharth.bhardwaj2051999@gmail.com',
            )
            

        except ClientError as e:
            print(e.response['Error']['Message'])
        else:
            print("Email sent! Message ID:"),
            print(response['MessageId'])
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
