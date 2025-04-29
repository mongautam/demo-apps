import pprint
from requests.auth import HTTPDigestAuth
import requests
import os
import sys
from constants import *
from dotenv import load_dotenv
from stream_processors_config import stream_processors, kafka_stream_processor

load_dotenv()  # Load environment variables from .env file

PUBLIC_KEY = os.environ["ATLAS_API_PUBLIC_KEY"]
PRIVATE_KEY = os.environ["ATLAS_API_PRIVATE_KEY"]
PROJECT_ID = os.environ["ATLAS_PROJECT_ID"]
STREAM_INSTANCE_NAME = os.environ["STREAM_PROCESSOR_INSTANCE_NAME"]

headers = {
    "Accept": "application/vnd.atlas.2024-05-30+json",
    "Content-Type": "application/json",
}

def start_processor(processor):
    print(f"\nStarting stream processor: {processor['name']}")
    response = requests.post(
        f"https://cloud.mongodb.com/api/atlas/v2/groups/{PROJECT_ID}/streams/{STREAM_INSTANCE_NAME}/processor/{processor['name']}:start",
        auth=HTTPDigestAuth(PUBLIC_KEY, PRIVATE_KEY),
        headers=headers
    )
    if not (200 <= response.status_code < 300):
        resp_json = response.json()
        if "already been started" in str(resp_json):
            print(f"Stream processor {processor['name']} is already running")
            return True
        print(f"Error starting stream processor {processor['name']}:")
        pprint.pprint(resp_json)
        return False
    pprint.pprint(response.json())
    return True

use_kafka = "--kafka" in sys.argv

if use_kafka:
    if not start_processor(kafka_stream_processor):
        sys.exit(1)
else:
    for processor in stream_processors:
        if not start_processor(processor):
            sys.exit(1)