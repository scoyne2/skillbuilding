## Example to use twitter api and feed data into kinesis

from TwitterAPI import TwitterAPI
import boto3
import json
import creds
import time


## twitter credentials

consumer_key = creds.consumer_key
consumer_secret = creds.consumer_secret
access_token_key = creds.access_token_key
access_token_secret = creds.access_token_secret
aws_access_key = creds.aws_access_key
aws_secret_key = creds.aws_secret_key

api = TwitterAPI(consumer_key, consumer_secret, access_token_key, access_token_secret)

kinesis = boto3.client('kinesis', 
						aws_access_key_id=aws_access_key,
					    aws_secret_access_key=aws_secret_key,
					    region_name='us-west-2')

r = api.request('statuses/filter', {'locations':'-74,40,-73,41'})

for item in r:
	kinesis.put_record(StreamName="twitter-stream", Data=json.dumps(item), PartitionKey="filler")
	print(json.dumps(item))
	time.sleep(5)