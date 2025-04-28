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
        "clusterType": "REPLICASET",
        "replicationSpecs": [
            {
              "regionConfigs": [
                {
                  "electableSpecs": {
                    "instanceSize": "M0",
                  },
                  "backingProviderName": "AWS",
                  "providerName": "TENANT",
                  "regionName": "US_EAST_1"
                }
              ]
            }
        ],
        "terminationProtectionEnabled": False
    }

    # Create cluster
    response = requests.post(
        f"{base_url}/groups/{project_id}/clusters",
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
            f"{base_url}/groups/{project_id}/clusters/{cluster_info['name']}",
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

def get_cluster_connection_info(project_id, cluster_name, public_key, private_key):
    """
    Fetch connection string, provider, and region for a cluster from Atlas API.
    Tries flexClusters API first, falls back to /clusters API if not found or missing connection string.
    Returns (connection_string, provider, region)
    """
    base_url = "https://cloud.mongodb.com/api/atlas/v2"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/vnd.atlas.2024-11-23+json",
    }
    flex_url = f"{base_url}/groups/{project_id}/flexClusters/{cluster_name}"
    resp = requests.get(flex_url, auth=HTTPDigestAuth(public_key, private_key), headers=headers)
    fallback = False

    if resp.status_code == 400:
        fallback = True
    else:
        resp.raise_for_status()
        cluster_info = resp.json()
        provider = cluster_info.get("providerSettings", {}).get("backingProviderName")
        region = cluster_info.get("providerSettings", {}).get("regionName")
        connection_string = cluster_info.get("connectionStrings", {}).get("standardSrv")
        if connection_string:
            return connection_string, provider, region
        else:
            fallback = True

    if fallback:
        clusters_url = f"{base_url}/groups/{project_id}/clusters/{cluster_name}"
        resp = requests.get(clusters_url, auth=HTTPDigestAuth(public_key, private_key), headers=headers)
        resp.raise_for_status()
        cluster_info = resp.json()
        provider = cluster_info.get("providerSettings", {}).get("providerName")
        region = cluster_info.get("providerSettings", {}).get("regionName")
        connection_string = cluster_info.get("connectionStrings", {}).get("standardSrv")
        return connection_string, provider, region

def list_all_clusters(project_id, pub_key, pri_key):
    """
    List all flexClusters and clusters in the given Atlas project.
    Returns a list of dicts with keys: name, type (flex/standard), provider, region, connection_string.
    """
    base_url = "https://cloud.mongodb.com/api/atlas/v2"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/vnd.atlas.2024-11-23+json",
    }
    clusters = []
    # List flexClusters
    flex_url = f"{base_url}/groups/{project_id}/flexClusters"
    resp = requests.get(flex_url, auth=HTTPDigestAuth(pub_key, pri_key), headers=headers)
    if resp.status_code == 200:
        for c in resp.json().get("results", []):
            clusters.append({
                "name": c["name"],
                "type": "flex",
                "provider": c.get("providerSettings", {}).get("backingProviderName"),
                "region": c.get("providerSettings", {}).get("regionName"),
                "connection_string": c.get("connectionStrings", {}).get("standardSrv"),
            })
    # List standard clusters
    std_url = f"{base_url}/groups/{project_id}/clusters"
    resp = requests.get(std_url, auth=HTTPDigestAuth(pub_key, pri_key), headers=headers)
    if resp.status_code == 200:
        for c in resp.json().get("results", []):
            # Extract provider and region from replicationSpecs if available
            provider = None
            region = None
            replication_specs = c.get("replicationSpecs", [])
            if replication_specs:
                region_configs = replication_specs[0].get("regionConfigs", [])
                if region_configs:
                    provider = region_configs[0].get("providerName")
                    region = region_configs[0].get("regionName")
            clusters.append({
                "name": c["name"],
                "type": "standard",
                "provider": provider,
                "region": region,
                "connection_string": c.get("connectionStrings", {}).get("standardSrv"),
            })
    return clusters
