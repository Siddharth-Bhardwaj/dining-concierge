import json
import boto3
import dateutil.parser
import re
import datetime
import math

def elicitSlot(slotToElicit, event, message):
    res = {
        "sessionAttributes": event['sessionAttributes'],
        "dialogAction": {
            "type": "ElicitSlot",
            "intentName": "DiningSuggestionsIntent",
            "slots": event['currentIntent']['slots'],
            "slotToElicit": slotToElicit,
            # "message": message
        }
    }
    return res

def confirmIntent(event):
    return {
        'sessionAttributes': event['sessionAttributes'],
        'dialogAction': {
            'type': 'ConfirmIntent',
            'intentName': "DiningSuggestionsIntent",
            'slots': event['currentIntent']['slots'],
            # 'message': "Are you sure you would like to proceed with the provided information?"
        }
    }
    
def parseInt(n):
    try:
        return int(n)
    except ValueError:
        return float('nan')
    
def isValidDate(date):
    try:
        parsedDate = dateutil.parser.parse(date).date()
        today = datetime.datetime.now().date()
        return parsedDate >= today
    except ValueError:
        return False
        
def isValidEmail(email):
    regex = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b'
    return re.fullmatch(regex, email)
    
def isValidTime(time):
    if len(time) != 5:
        return False

    hour, minute = time.split(':')
    hour = parseInt(hour)
    minute = parseInt(minute)
        
    if math.isnan(hour) or math.isnan(minute):
        return False

    if hour < 9 or hour > 24:
        return False
        
    return True
    
def validateAndConfirm(event):
    validLocations = ['New York', 'NY', 'ny', 'new york', 'New york', 'new York', 'manhattan', "Manhattan", "New York City", "new york city", "New york city",
    "new York city", "new york City", "New York city", "New york City", "new York City", "nyc", "NYC", "Nyc"]
    validCuisines = ["Indian", "indian", "Mexican", "mexican", "Chinese", "chinese", "Japanese", "japanese", "Italian", "italian"]
    
    slots = event['currentIntent']['slots']
    
    if (slots['Location'] is None):
        return elicitSlot("Location", event, "Which location would you prefer?")
    elif (slots['Location'] not in validLocations):
        return elicitSlot("Location", event, "Which location would you prefer?")

    if (slots['Cuisine'] is None):
        return elicitSlot("Cuisine", event, "What cuisine would you prefer?")
    elif (slots['Cuisine'] not in validCuisines):
        return elicitSlot("Cuisine", event, "What cuisine would you prefer?")
        
    if (slots['DiningDate'] is None):
        return elicitSlot("DiningDate", event, "Which date would you prefer?")
    elif (isValidDate(slots['DiningDate']) == False):
        return elicitSlot("DiningDate", event, "Which date would you prefer?")
        
    if (slots['DiningTime'] is None):
        return elicitSlot("DiningTime", event, "What time would you prefer?")
    elif (isValidTime(slots['DiningTime']) == False):
        return elicitSlot("DiningTime", event, "What time would you prefer?")
        
    if (slots['NumberOfPeople'] is None):
        return elicitSlot("NumberOfPeople", event, "How many people should the reservation be made for?")
    elif (math.isnan(parseInt(slots['NumberOfPeople'])) == True or parseInt(slots['NumberOfPeople']) < 1):
        return elicitSlot("NumberOfPeople", event, "How many people should the reservation be made for?")
        
    if (slots['Email'] is None):
        return elicitSlot("Email", event, "Please provide the email address you would like me to send the recommendations to.")
    elif (isValidEmail(slots['Email']) == False):
        return elicitSlot("Email", event, "Please provide the email address you would like me to send the recommendations to.")
        
    return confirmIntent(event)
    
def pushToSqs(event):
    sqsClient = boto3.client('sqs')
    
    sqsResponse = sqsClient.send_message(
        QueueUrl='https://sqs.us-east-1.amazonaws.com/550673492243/diningConciergeQueue',
        DelaySeconds=0,
        MessageAttributes={
            'location': {
                'DataType': 'String',
                'StringValue': event['currentIntent']['slots']['Location']
            },
            'cuisine': {
                'DataType': 'String',
                'StringValue': event['currentIntent']['slots']['Cuisine']
            },
            'date': {
                'DataType': 'String',
                'StringValue': event['currentIntent']['slots']['DiningDate']
            },
            'time': {
                'DataType': 'String',
                'StringValue': event['currentIntent']['slots']['DiningTime']
            },
            'num_people': {
                'DataType': 'Number',
                'StringValue': event['currentIntent']['slots']['NumberOfPeople']
            },
            'email': {
                'DataType': 'String',
                'StringValue': event['currentIntent']['slots']['Email']
            }
        },
        MessageBody=(
            'dining suggestions slots'
        )
    )
    
    if sqsResponse is not None:
        return {
            "dialogAction": {
                "fulfillmentState":"Fulfilled",
                "type":"Close",
                "message": {
                    "contentType":"PlainText",
                    "content": "You will receive your suggestions on the provided email address shortly. Thank you!"
                }
            }
        }
    
    return {
        "dialogAction": {
            "fulfillmentState":"Fulfilled",
            "type":"Close",
            "message": {
                "contentType":"PlainText",
                "content": "Something went wrong. Please try again."
            }
        }
    }
    
def cancelRequest(event):
    return {
        "dialogAction": {
            "fulfillmentState":"Fulfilled",
            "type":"Close",
            "message": {
                "contentType":"PlainText",
                "content": "Okay, your request has been canceled."
            }
        }
    }
    
def lambda_handler(event, context):
    print(event)
    if (event['currentIntent']['name'] == 'GreetingIntent'):
        return {
            "dialogAction": {
                "fulfillmentState":"Fulfilled",
                "type":"Close",
                "message": {
                    "contentType":"PlainText",
                    "content": "Hi! How can I help you?"
                }
            }
        }
    
    if (event['currentIntent']['name'] == 'ThankYouIntent'):
        return {
            "dialogAction": {
                "fulfillmentState":"Fulfilled",
                "type":"Close",
                "message": {
                    "contentType":"PlainText",
                    "content": "You're welcome!"
                }
            }
        }
        
    if (event is not None and event['currentIntent']['confirmationStatus'] == "Confirmed"):
        return pushToSqs(event)
    if (event is not None and event['currentIntent']['confirmationStatus'] == "Denied"):
        return cancelRequest(event)
    if (event is not None and event['currentIntent'] is not None):
        return validateAndConfirm(event)
        
    return {
        'dialogAction': {
            'type': 'Delegate'
        }
    }
