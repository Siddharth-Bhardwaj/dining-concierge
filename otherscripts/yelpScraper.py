import requests
import boto3
from botocore.exceptions import ClientError
from decimal import Decimal
import datetime
import json
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

# Diningconcierg@3

API_KEY = '22YCP0c4P6em6tinrfAsVnX93OngJvDEFSkk2LESgpGpRn8-3SLwhD8W65TJt7bvvSQT2OPHLcMqeOQu8jZqmCxXGMJPpZ0J3RWsrVW60pw25CZXLjVTMvoOMyrRZXYx'
ENDPOINT = 'https://api.yelp.com/v3/businesses/search'
HEADERS = {'Authorization': f'Bearer 22YCP0c4P6em6tinrfAsVnX93OngJvDEFSkk2LESgpGpRn8-3SLwhD8W65TJt7bvvSQT2OPHLcMqeOQu8jZqmCxXGMJPpZ0J3RWsrVW60pw25CZXLjVTMvoOMyrRZXYx'}
credentials = boto3.Session(region_name='us-east-1', aws_access_key_id='', aws_secret_access_key='').get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, 'us-east-1', 'es')
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
esClient = Elasticsearch(
        hosts=["https://search-dining-concierge-sfd4ecm7m3s4behekq7gyck64e.aos.us-east-1.on.aws:443"],
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection,
        http_auth=('dining-concierge', 'Diningconcierg@3')
        )
table = dynamodb.Table('yelp-restaurants')

print(credentials)
print(awsauth)
print(esClient.info)

cuisine_types = ['Indian', 'Chinese', 'Mexican', 'Italian', 'Japanese']

for cuisine_type in cuisine_types:
    offset = 0
    for i in range(0, 50):
        offset += 50
        PARAMETERS = {
            'term': f'{cuisine_type} restaurant',
            'location': 'Manhattan',
            'radius': 40000,
            'limit': 50,
            'offset': offset
        }
        response = requests.get(url=ENDPOINT, params=PARAMETERS, headers=HEADERS)
        business_data = response.json()
        # with open('data.json', 'w') as f:
        #     json.dump(business_data, f)

        if (business_data is not None and "businesses" in business_data and len(business_data['businesses']) > 0):
            for biz in business_data['businesses']:
                try:
                    table.put_item(
                        Item={
                            'businessId': biz['id'],
                            'name': biz['name'],
                            'cuisine': cuisine_type,
                            'category': biz['categories'][0]['alias'],
                            'rating': Decimal(str(biz['rating'])),
                            'latitude': Decimal(str(biz['coordinates']['latitude'])),
                            'longitude': Decimal(str(biz['coordinates']['longitude'])),
                            'address': biz['location']['address1'],
                            'city': biz['location']['city'],
                            'zipcode': biz['location']['zip_code'],
                            'phone': biz['phone'],
                            'url': str(biz['url']),
                            'reviewCount': biz['review_count'],
                            'insertedAtTimestamp': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        },
                        ConditionExpression='attribute_not_exists(businessId)' # to prevent duplicates
                    )
                    esClient.index(index='restaurant', doc_type='Restaurant', body={
                        'id' : biz['id'],
                        'cuisine' : cuisine_type,
                    })
                except ClientError as e:
                    print('here')
                    print(e.response['Error']['Code'])