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
API_URL = f"https://cloud.mongodb.com/api/atlas/v2/groups/{PROJECT_ID}/streams/{STREAM_INSTANCE_NAME}/processor"

headers = {
    "Accept": "application/vnd.atlas.2024-05-30+json",
    "Content-Type": "application/json",
}

# Create each stream processor and print the response
for processor in stream_processors + [kafka_stream_processor]:
    print(f"\nCreating stream processor: {processor['name']}")
    response = requests.post(
        API_URL,
        auth=HTTPDigestAuth(PUBLIC_KEY, PRIVATE_KEY),
        headers=headers,
        json=processor,
    )
    if not (200 <= response.status_code < 300):
        if response.status_code == 409 and response.json().get('errorCode') == 'STREAM_PROCESSOR_NAME_ALREADY_EXISTS':
            print(f"Stream processor {processor['name']} already exists, skipping...")
            continue
        print(f"Error creating stream processor {processor['name']}:")
        pprint.pprint(response.json())
        sys.exit(1)
    pprint.pprint(response.json())
