import requests
import time
from requests.auth import HTTPDigestAuth

DEFAULT_CLUSTER_NAME = "OrderFulfillmentDemoCluster"

def create_cluster(project_id, pub_key, pri_key):
    """Create a minimal M0 free tier cluster in Atlas."""
    base_url = "https://cloud.mongodb.com/api/atlas/v2"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/vnd.atlas.2024-11-23+json",
    }

    # Basic M0 cluster configuration
    cluster_config = {
        "name": DEFAULT_CLUSTER_NAME,
        "providerSettings": {
            "backingProviderName": "AWS",
            "regionName": "US_EAST_1"
        },
        "terminationProtectionEnabled": False
    }

    # Create cluster
    response = requests.post(
        f"{base_url}/groups/{project_id}/flexClusters",
        auth=HTTPDigestAuth(pub_key, pri_key),
        headers=headers,
        json=cluster_config,
    )

    if response.status_code not in [201, 200]:
        raise Exception(f"Failed to create cluster: {response.text}")

    cluster_info = response.json()

    # Wait for cluster to be ready (timeout after 5 minutes)
    spinner = ['|', '/', '-', '\\']
    idx = 0
    timeout = time.time() + 5 * 60
    print("Waiting for cluster to be ready ", end='', flush=True)
    while time.time() < timeout:
        status_response = requests.get(
            f"{base_url}/groups/{project_id}/flexClusters/{cluster_info['name']}",
            auth=HTTPDigestAuth(pub_key, pri_key),
            headers=headers,
        )
        status = status_response.json()["stateName"]
        print(f"\rWaiting for cluster to be ready {spinner[idx % len(spinner)]}", end='', flush=True)
        idx += 1
        if status == "IDLE":
            print("\rCluster is ready!           ")
            return cluster_info["name"]
        time.sleep(10)

    print()
    raise Exception("Cluster creation timed out")

def get_mongo_connection_string(project_id, cluster_name, public_key, private_key):
    """Fetch the connection string for a cluster from the Atlas API."""
    url = f"https://cloud.mongodb.com/api/atlas/v2/groups/{project_id}/flexClusters/{cluster_name}"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/vnd.atlas.2024-11-23+json",
    }
    resp = requests.get(url, auth=HTTPDigestAuth(public_key, private_key), headers=headers)
    resp.raise_for_status()
    return resp.json()["connectionStrings"]["standardSrv"]
