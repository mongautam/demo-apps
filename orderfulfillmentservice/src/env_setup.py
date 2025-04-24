"""Environment setup and configuration functions."""
import os
import sys
import subprocess
import importlib.util
from pathlib import Path

ENV_FILE = ".env"
ENV_TEMPLATE = "env"
REQUIRED_PACKAGES = ["pymongo", "requests", "flask"]

ENV_VARS = {
    "MONGO_USER": "MongoDB Atlas database user (e.g. order_fulfillment_user)",
    "MONGO_PASS": "MongoDB Atlas database password (e.g. strongpassword123)",
    "ATLAS_PROJECT_ID": "Atlas project ID (see Atlas dashboard)",
    "ATLAS_CLUSTER_NAME": "Atlas cluster name (see Atlas dashboard)",
    "MONGO_URL": "Standard MongoDB connection string (e.g. mongodb+srv://...)",
    "MONGO_FULL_URL": "Full MongoDB connection string, including credentials",
    "ATLAS_API_PUBLIC_KEY": "Atlas API public key (see Atlas dashboard)",
    "ATLAS_API_PRIVATE_KEY": "Atlas API private key (see Atlas dashboard)",
    "STREAM_PROCESSOR_INSTANCE_NAME": "Unique name for stream processor instance",
    "ATLAS_STREAM_PROCESSOR_URL": "URL for Atlas Stream Processing endpoint",
    "ORDER_SERVICE_URL": "Order service endpoint",
    "KAFKA_BOOTSTRAP_SERVERS": "Kafka bootstrap servers",
    "KAFKA_USERNAME": "Kafka username (if SASL/PLAIN auth is used)",
    "KAFKA_PASSWORD": "Kafka password",
    "KAFKA_SHOPPING_CART_TOPIC": "Kafka topic for shopping cart events",
    "SHOPPING_CART_DB_NAME": "Shopping cart MongoDB database name",
    "SHOPPING_CART_COLLECTION_NAME": "Shopping cart MongoDB collection name",
}

KAFKA_ENV_KEYS = [
    "KAFKA_BOOTSTRAP_SERVERS",
    "KAFKA_USERNAME",
    "KAFKA_PASSWORD",
    "KAFKA_SHOPPING_CART_TOPIC"
]

def check_dependencies():
    """Check if required packages are installed."""
    missing = []
    for package in REQUIRED_PACKAGES:
        if importlib.util.find_spec(package) is None:
            missing.append(package)
    
    if missing:
        print("Error: Missing required dependencies:")
        for pkg in missing:
            print(f"  - {pkg}")
        print("\nPlease install dependencies before running this script:")
        print("pip install -r requirements.txt")
        sys.exit(1)

def setup_environment():
    """Set up the environment (env file, variables, venv)."""
    if not Path(ENV_FILE).exists():
        if Path(ENV_TEMPLATE).exists():
            print(f"Copying {ENV_TEMPLATE} to {ENV_FILE}...")
            subprocess.run(["cp", ENV_TEMPLATE, ENV_FILE])
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
    check_venv()
    check_dependencies()
    return env_vars

def check_venv():
    """Check and setup virtual environment if needed."""
    if sys.prefix == sys.base_prefix:
        venv_path = Path("venv/bin/activate")
        if venv_path.exists():
            subprocess.run(["bash", "-c", "source venv/bin/activate && exec python3 " + " ".join(sys.argv)])
            sys.exit(0)
        else:
            print("Virtual environment not found. Creating...")
            subprocess.run([sys.executable, "-m", "venv", "venv"])
            print("Please activate the virtual environment and install dependencies:")
            print("source venv/bin/activate")
            print("pip install -r requirements.txt")
            print("Then rerun this script.")
            sys.exit(1)

def prompt_for_env_vars(env_vars, only_kafka=False):
    """Prompt user for missing environment variables and save to .env file."""
    updated = False
    vars_to_prompt = KAFKA_ENV_KEYS if only_kafka else list(ENV_VARS.keys())
    
    for var in vars_to_prompt:
        if var not in env_vars or not env_vars[var] or env_vars[var].strip() == "":
            while True:
                print(f"\nMissing value for {var}.")
                hint = ENV_VARS.get(var)
                if hint:
                    print(f"Hint: {hint}")
                value = input(f"Enter value for {var}: ").strip()
                if value and not value.isspace():  # Check for non-empty and non-whitespace values
                    env_vars[var] = value
                    updated = True
                    os.environ[var] = value
                    break
                print("Error: Empty or whitespace-only value not allowed. Please enter a valid value.")
    
    if updated:
        with open(ENV_FILE, "w") as f:
            for k, v in env_vars.items():
                if v:  # Only write non-empty values
                    f.write(f'{k}="{v}"\n')
        print(f"Updated {ENV_FILE} with provided values.")