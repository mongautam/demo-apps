import pprint
from requests.auth import HTTPDigestAuth
import requests
import os

from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env file

PUBLIC_KEY = os.environ["ATLAS_API_PUBLIC_KEY"]
PRIVATE_KEY = os.environ["ATLAS_API_PRIVATE_KEY"]
PROJECT_ID = os.environ["ATLAS_PROJECT_ID"]
STREAM_INSTANCE_NAME = os.environ["STREAM_PROCESSOR_INSTANCE_NAME"]
CLOUD_PROVIDER = os.environ["CLOUD_PROVIDER"]
CLOUD_REGION = os.environ["CLOUD_REGION"]

API_URL = f"https://cloud.mongodb.com/api/atlas/v2/groups/{PROJECT_ID}/streams"

headers = {
    "Accept": "application/vnd.atlas.2024-05-30+json",
    "Content-Type": "application/json",
}


data = {
    "dataProcessRegion": {"cloudProvider": CLOUD_PROVIDER, "region": CLOUD_REGION},
    "name": STREAM_INSTANCE_NAME,
    "sampleConnections": {"solar": False},
    "streamConfig": {"tier": "SP10"},
}

response = requests.post(
    API_URL, auth=HTTPDigestAuth(PUBLIC_KEY, PRIVATE_KEY), headers=headers, json=data
)
pprint.pprint(response.json())
