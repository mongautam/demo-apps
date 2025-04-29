import pprint
import sys
from requests.auth import HTTPDigestAuth
import requests
import os

from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env file

PUBLIC_KEY = os.environ["ATLAS_API_PUBLIC_KEY"]
PRIVATE_KEY = os.environ["ATLAS_API_PRIVATE_KEY"]
PROJECT_ID = os.environ["ATLAS_PROJECT_ID"]
STREAM_INSTANCE_NAME = os.environ["STREAM_PROCESSOR_INSTANCE_NAME"]
ORDER_SERVICE_URL = os.environ["ORDER_SERVICE_URL"]
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_USERNAME = os.getenv("KAFKA_USERNAME", "admin")
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD", "admin")
ATLAS_CLUSTER_NAME = os.environ["ATLAS_CLUSTER_NAME"]

API_URL = f"https://cloud.mongodb.com/api/atlas/v2/groups/{PROJECT_ID}/streams/{STREAM_INSTANCE_NAME}/connections"

headers = {
    "Accept": "application/vnd.atlas.2024-05-30+json",
    "Content-Type": "application/json",
}

send_headers = {
    "Content-Type": "application/json",
    "ngrok-skip-browser-warning": "true",
}

# Define all connections in an array
connections = [
    {
        "name": "mongoDBSink",
        "type": "Cluster",
        "clusterName": ATLAS_CLUSTER_NAME,
        "dbRoleToExecute": {"role": "atlasAdmin", "type": "BUILT_IN"},
    },
    {
        "name": "flaskService",
        "type": "Https",
        "headers": send_headers,
        "url": ORDER_SERVICE_URL,
    },
    {
        "name": "shoppingCartKafkaEventSource",
        "type": "Kafka",
        "bootstrapServers": KAFKA_BOOTSTRAP_SERVERS,
        "authentication": {
            "mechanism": "PLAIN",
            "username": KAFKA_USERNAME,
            "password": KAFKA_PASSWORD,
        },
        "security": {
            "protocol": "SASL_PLAINTEXT",
        }
    },
]

# Create each connection and print the response
for connection in connections:
    print(f"\nCreating connection: {connection['name']}")
    response = requests.post(
        API_URL,
        auth=HTTPDigestAuth(PUBLIC_KEY, PRIVATE_KEY),
        headers=headers,
        json=connection,
    )
    if not (200 <= response.status_code < 300):
        if response.status_code == 409 and response.json().get('errorCode') == 'STREAM_CONNECTION_NAME_ALREADY_EXISTS':
            print(f"Connection {connection['name']} already exists, skipping...")
            continue
        print(f"Error creating connection {connection['name']}:")
        pprint.pprint(response.json())
        sys.exit(1)
    pprint.pprint(response.json())
