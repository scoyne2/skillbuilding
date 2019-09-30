import boto3
from datetime import datetime
import json
import calendar
import random
import time

my_stream_name = 'scoyne_test'
AWS_PROFILE = "personal-stream"
session = boto3.Session(profile_name=AWS_PROFILE)
kinesis_client = session.client('kinesis', region_name='us-west-1')


def put_to_stream(thing_id, property_value, property_timestamp):
    payload = {
                'prop': str(property_value),
                'timestamp': str(property_timestamp),
                'thing_id': thing_id
              }

    print payload

    kinesis_client.put_record(
        StreamName=my_stream_name,
        Data=json.dumps(payload),
        PartitionKey=thing_id)


while True:
    property_value = random.randint(40, 120)
    property_timestamp = calendar.timegm(datetime.utcnow().timetuple())
    thing_id = 'aa-bb'

    put_to_stream(thing_id, property_value, property_timestamp)

    # wait for 5 second
    time.sleep(5)
