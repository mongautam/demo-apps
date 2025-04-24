"""Environment setup and configuration functions."""
import os
import sys
from pathlib import Path
from .atlas_api import create_cluster, list_all_clusters

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
            set_connection_env_vars(cluster_name, mongo_url, provider, region)
        except Exception as e:
            print(f"Error: Could not fetch connection string or cluster info from Atlas API: {e}")
            sys.exit(1)
    else:
        print("Error: Missing Atlas credentials or cluster name, cannot update MONGO_URL.")
        sys.exit(1)

def set_connection_env_vars(cluster_name, connection_string, provider, region):
    """Helper to update MONGO_URL, CLOUD_PROVIDER, and CLOUD_REGION from cluster info."""
    if connection_string:
        mongo_url = connection_string
        if mongo_url.startswith('mongodb+srv://'):
            mongo_url = mongo_url[len('mongodb+srv://'):]
        at_index = mongo_url.find('@')
        if at_index != -1:
            mongo_url = '@' + mongo_url[at_index+1:]
        else:
            mongo_url = '@' + mongo_url
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

def set_env_var(var, value, env_vars):
    env_vars[var] = value
    os.environ[var] = value
    update_env_file(var, value)

def prompt_and_set_env_var(var, desc, env_vars):
    """Prompt user for a variable, update env and .env file."""
    while True:
        print(f"\nMissing value for {var}.")
        if desc:
            print(f"Hint: {desc}")
        value = input(f"Enter value for {var}: ").strip()
        if value and not value.isspace():
            set_env_var(var, value, env_vars)
            print(f"Updated {ENV_FILE} with new value for {var}")
            break
        print("Error: Empty or whitespace-only value not allowed. Please enter a valid value.")

def select_or_create_cluster(env_vars):
    while True:
        print("\nNo Atlas cluster name found.")
        print("Would you like the driver to create the cluster? ... (y/n): ", end='')
        choice = input().strip().lower()
        if choice == 'y':
            try:
                cluster_name = create_cluster(
                    env_vars['ATLAS_PROJECT_ID'],
                    env_vars['ATLAS_API_PUBLIC_KEY'],
                    env_vars['ATLAS_API_PRIVATE_KEY']
                )
                print(f"Created cluster: {cluster_name}")
                set_env_var('ATLAS_CLUSTER_NAME', cluster_name, env_vars)
                # Get connection info and set env vars
                from .atlas_api import get_cluster_connection_info
                connection_string, provider, region = get_cluster_connection_info(
                    env_vars['ATLAS_PROJECT_ID'],
                    cluster_name,
                    env_vars['ATLAS_API_PUBLIC_KEY'],
                    env_vars['ATLAS_API_PRIVATE_KEY']
                )
                set_connection_env_vars(cluster_name, connection_string, provider, region)
                return
            except Exception as e:
                print(f"Error creating cluster: {e}")
        elif choice == 'n':
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
                    cluster = clusters[int(sel)-1]
                    cluster_name = cluster['name']
                    set_env_var('ATLAS_CLUSTER_NAME', cluster_name, env_vars)
                    connection_string = cluster.get('connection_string')
                    provider = cluster.get('provider')
                    region = cluster.get('region')
                    if connection_string:
                        set_connection_env_vars(cluster_name, connection_string, provider, region)
                    else:
                        from .atlas_api import get_cluster_connection_info
                        connection_string, provider, region = get_cluster_connection_info(
                            env_vars['ATLAS_PROJECT_ID'],
                            cluster_name,
                            env_vars['ATLAS_API_PUBLIC_KEY'],
                            env_vars['ATLAS_API_PRIVATE_KEY']
                        )
                        set_connection_env_vars(cluster_name, connection_string, provider, region)
                    return
                print("Invalid selection. Please enter a valid number.")
            break
        else:
            print("Please enter 'y' or 'n'.")

def prompt_for_env_vars(env_vars, only_kafka=False, only_vars=None):
    """Prompt user for required environment variables and save to .env file."""
    # Only prompt for vars with prompt=True
    prompt_vars = [k for k, v in ENV_VARS.items() if v.get("prompt")]
    # Always prompt for API keys and project id first
    for var in ["ATLAS_API_PUBLIC_KEY", "ATLAS_API_PRIVATE_KEY", "ATLAS_PROJECT_ID"]:
        if not env_vars.get(var):
            prompt_and_set_env_var(var, ENV_VARS[var]['desc'], env_vars)
    # Cluster name logic (populate ATLAS_CLUSTER_NAME and MONGO_URL)
    cluster_name = env_vars.get('ATLAS_CLUSTER_NAME')
    if not cluster_name:
        select_or_create_cluster(env_vars)
    else:
        # If already populated, always update MONGO_URL
        update_mongo_url_with_cluster(cluster_name)
    # Prompt for the rest of the required vars (excluding those already handled)
    for var in prompt_vars:
        if var in ["ATLAS_API_PUBLIC_KEY", "ATLAS_API_PRIVATE_KEY", "ATLAS_PROJECT_ID"]:
            continue
        if var not in env_vars or not env_vars[var] or env_vars[var].strip() == "":
            prompt_and_set_env_var(var, ENV_VARS[var]["desc"], env_vars)