"""Environment setup and configuration functions."""
import os
import sys
from pathlib import Path
from .atlas_api import create_cluster

ENV_FILE = ".env"
ENV_TEMPLATE = "env"
REQUIRED_PACKAGES = ["pymongo", "requests", "flask"]

ENV_VARS = {
    "MONGO_USER": {"desc": "MongoDB Atlas database user (e.g. order_fulfillment_user)", "prompt": True},
    "MONGO_PASS": {"desc": "MongoDB Atlas database password (e.g. strongpassword123)", "prompt": True},
    "ATLAS_PROJECT_ID": {"desc": "Atlas project ID (see Atlas dashboard)", "prompt": True},
    "ATLAS_CLUSTER_NAME": {"desc": "Atlas cluster name (see Atlas dashboard)", "prompt": False},
    "MONGO_URL": {"desc": "Standard MongoDB connection string (e.g. mongodb+srv://...)", "prompt": False},
    "ATLAS_API_PUBLIC_KEY": {"desc": "Atlas API public key (see Atlas dashboard)", "prompt": True},
    "ATLAS_API_PRIVATE_KEY": {"desc": "Atlas API private key (see Atlas dashboard)", "prompt": True},
    "STREAM_PROCESSOR_INSTANCE_NAME": {"desc": "Unique name for stream processor instance", "prompt": True},
    "ATLAS_STREAM_PROCESSOR_URL": {"desc": "URL for Atlas Stream Processing endpoint", "prompt": False},
    "ORDER_SERVICE_URL": {"desc": "Order service endpoint", "prompt": False},
    "KAFKA_BOOTSTRAP_SERVERS": {"desc": "Kafka bootstrap servers", "prompt": False},
    "KAFKA_USERNAME": {"desc": "Kafka username (if SASL/PLAIN auth is used)", "prompt": True},
    "KAFKA_PASSWORD": {"desc": "Kafka password", "prompt": True},
    "KAFKA_SHOPPING_CART_TOPIC": {"desc": "Kafka topic for shopping cart events", "prompt": False},
    "SHOPPING_CART_DB_NAME": {"desc": "Shopping cart MongoDB database name", "prompt": False},
    "SHOPPING_CART_COLLECTION_NAME": {"desc": "Shopping cart MongoDB collection name", "prompt": False},
}

KAFKA_ENV_KEYS = [
    "KAFKA_BOOTSTRAP_SERVERS",
    "KAFKA_USERNAME",
    "KAFKA_PASSWORD",
    "KAFKA_SHOPPING_CART_TOPIC"
]

def setup_environment():
    """Set up the environment (env file, variables)."""
    if not Path(ENV_FILE).exists():
        if Path(ENV_TEMPLATE).exists():
            print(f"Copying {ENV_TEMPLATE} to {ENV_FILE} with empty values...")
            with open(ENV_TEMPLATE) as src, open(ENV_FILE, 'w') as dst:
                for line in src:
                    if line.strip() == '' or line.strip().startswith('#'):
                        dst.write(line)
                    else:
                        var = line.split('=')[0].strip()
                        dst.write(f'{var}=""\n')
        else:
            print(f"Missing both {ENV_FILE} and {ENV_TEMPLATE}. Please provide one.")
            sys.exit(1)
    
    env_vars = {}
    with open(ENV_FILE) as f:
        for line in f:
            if line.strip() and not line.strip().startswith("#"):
                try:
                    k, v = line.strip().split("=", 1)
                    env_vars[k] = v.strip().strip('"').strip("'")
                except ValueError:
                    continue
    
    os.environ.update(env_vars)
    return env_vars

def update_env_file(var_name, value):
    """Update a single variable in the .env file, preserving order and comments."""
    lines = []
    found = False
    if Path(ENV_FILE).exists():
        with open(ENV_FILE) as f:
            lines = f.readlines()
    new_lines = []
    for line in lines:
        if line.strip().startswith(f"{var_name}="):
            # Preserve the original line ending (\n or not)
            line_ending = '\n' if line.endswith('\n') else ''
            new_lines.append(f'{var_name}="{value}"{line_ending}')
            found = True
        else:
            new_lines.append(line)
    if not found:
        # Append at the end if not found, with a newline if needed
        if new_lines and not new_lines[-1].endswith('\n'):
            new_lines[-1] = new_lines[-1] + '\n'
        new_lines.append(f'{var_name}="{value}"\n')
    with open(ENV_FILE, "w") as f:
        f.writelines(new_lines)

def update_mongo_url_with_cluster(cluster_name):
    """Update MONGO_URL in .env to use the real connection string from Atlas API.
    Also populate provider and region from Atlas API."""
    project_id = os.environ.get('ATLAS_PROJECT_ID')
    public_key = os.environ.get('ATLAS_API_PUBLIC_KEY')
    private_key = os.environ.get('ATLAS_API_PRIVATE_KEY')
    if project_id and public_key and private_key and cluster_name:
        try:
            from .atlas_api import get_cluster_connection_info
            mongo_url, provider, region = get_cluster_connection_info(project_id, cluster_name, public_key, private_key)
            # Remove 'mongodb+srv://' if present
            if mongo_url and mongo_url.startswith('mongodb+srv://'):
                mongo_url = mongo_url[len('mongodb+srv://'):]
            # Ensure it starts with '@'
            if mongo_url:
                at_index = mongo_url.find('@')
                if at_index != -1:
                    mongo_url = '@' + mongo_url[at_index+1:]
                else:
                    mongo_url = '@' + mongo_url
                # Ensure it ends with the correct suffix
                suffix = f'?retryWrites=true&w=majority&appName={cluster_name}'
                mongo_url = mongo_url.split('?', 1)[0] + suffix
                update_env_file('MONGO_URL', mongo_url)
                os.environ['MONGO_URL'] = mongo_url
            if provider:
                update_env_file('CLOUD_PROVIDER', provider)
                os.environ['CLOUD_PROVIDER'] = provider
            if region:
                update_env_file('CLOUD_REGION', region)
                os.environ['CLOUD_REGION'] = region
        except Exception as e:
            print(f"Error: Could not fetch connection string or cluster info from Atlas API: {e}")
            sys.exit(1)
    else:
        print("Error: Missing Atlas credentials or cluster name, cannot update MONGO_URL.")
        sys.exit(1)

def prompt_for_env_vars(env_vars, only_kafka=False, only_vars=None):
    """Prompt user for required environment variables and save to .env file."""
    # Only prompt for vars with prompt=True
    prompt_vars = [k for k, v in ENV_VARS.items() if v.get("prompt")]
    # Always prompt for API keys and project id first
    for var in ["ATLAS_API_PUBLIC_KEY", "ATLAS_API_PRIVATE_KEY", "ATLAS_PROJECT_ID"]:
        if not env_vars.get(var):
            while True:
                print(f"\nMissing value for {var}.")
                print(f"Hint: {ENV_VARS[var]['desc']}")
                value = input(f"Enter value for {var}: ").strip()
                if value:
                    env_vars[var] = value
                    os.environ[var] = value
                    update_env_file(var, value)
                    break
                print("Error: Empty value not allowed.")
    # Cluster name logic (populate ATLAS_CLUSTER_NAME and MONGO_URL)
    cluster_name = env_vars.get('ATLAS_CLUSTER_NAME')
    if not cluster_name:
        while True:
            print("\nNo Atlas cluster name found.")
            print("Would you like the driver to create the cluster? This will use your Atlas API credentials to provision a new flex-tier cluster in your project. (y/n): ", end='')
            choice = input().strip().lower()
            if choice == 'y':
                try:
                    cluster_name = create_cluster(
                        env_vars['ATLAS_PROJECT_ID'],
                        env_vars['ATLAS_API_PUBLIC_KEY'],
                        env_vars['ATLAS_API_PRIVATE_KEY']
                    )
                    print(f"Created cluster: {cluster_name}")
                    update_env_file('ATLAS_CLUSTER_NAME', cluster_name)
                    env_vars['ATLAS_CLUSTER_NAME'] = cluster_name
                    os.environ['ATLAS_CLUSTER_NAME'] = cluster_name
                    update_mongo_url_with_cluster(cluster_name)
                    break
                except Exception as e:
                    print(f"Error creating cluster: {e}")
            elif choice == 'n':
                from .atlas_api import list_all_clusters
                clusters = list_all_clusters(
                    env_vars['ATLAS_PROJECT_ID'],
                    env_vars['ATLAS_API_PUBLIC_KEY'],
                    env_vars['ATLAS_API_PRIVATE_KEY']
                )
                if not clusters:
                    print("No clusters found in your Atlas project. Please create one in the Atlas UI first.")
                    sys.exit(1)
                print("\nAvailable clusters:")
                for idx, c in enumerate(clusters, 1):
                    print(f"  {idx}. {c['name']} (type: {c['type']}, provider: {c['provider']}, region: {c['region']})")
                while True:
                    sel = input(f"Select a cluster by number (1-{len(clusters)}): ").strip()
                    if sel.isdigit() and 1 <= int(sel) <= len(clusters):
                        cluster_name = clusters[int(sel)-1]['name']
                        update_env_file('ATLAS_CLUSTER_NAME', cluster_name)
                        env_vars['ATLAS_CLUSTER_NAME'] = cluster_name
                        os.environ['ATLAS_CLUSTER_NAME'] = cluster_name
                        update_mongo_url_with_cluster(cluster_name)
                        break
                    print("Invalid selection. Please enter a valid number.")
                break
            else:
                print("Please enter 'y' or 'n'.")
    else:
        # If already populated, always update MONGO_URL
        update_mongo_url_with_cluster(cluster_name)
    # Prompt for the rest of the required vars (excluding those already handled)
    for var in prompt_vars:
        if var in ["ATLAS_API_PUBLIC_KEY", "ATLAS_API_PRIVATE_KEY", "ATLAS_PROJECT_ID"]:
            continue
        if var not in env_vars or not env_vars[var] or env_vars[var].strip() == "":
            while True:
                print(f"\nMissing value for {var}.")
                hint = ENV_VARS[var]["desc"]
                if hint:
                    print(f"Hint: {hint}")
                value = input(f"Enter value for {var}: ").strip()
                if value and not value.isspace():
                    env_vars[var] = value
                    os.environ[var] = value
                    update_env_file(var, value)
                    print(f"Updated {ENV_FILE} with new value for {var}")
                    break
                print("Error: Empty or whitespace-only value not allowed. Please enter a valid value.")