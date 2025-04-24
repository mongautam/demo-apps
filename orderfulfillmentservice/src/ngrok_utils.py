"""ngrok configuration and management functions."""
import os
import re
import time
import tempfile
import subprocess
from pathlib import Path

NGROK_CONFIG = ".ngrok.yml"

def check_ngrok_installed():
    """Check if ngrok is installed and accessible."""
    try:
        result = subprocess.run(["which", "ngrok"], capture_output=True, text=True, check=False)
        return result.returncode == 0
    except Exception:
        return False

def setup_ngrok(env_vars, debug=False):
    """Set up ngrok configuration."""
    if not check_ngrok_installed():
        print("\nngrok is not installed or not in your PATH.")
        print("Please install ngrok from https://ngrok.com/ and add it to your PATH.")
        print("Installation instructions: https://ngrok.com/download")
        sys.exit(1)
    
    print("\nngrok authentication token needed for setup.")
    print("You can get your auth token from https://dashboard.ngrok.com/get-started/your-authtoken")
    auth_token = input("Enter your ngrok auth token: ")
    
    print("\nCreating local ngrok configuration file...")
    with open(NGROK_CONFIG, "w") as f:
        f.write("version: 2\n")
        f.write(f"authtoken: {auth_token}\n")
        f.write("tunnels:\n")
        f.write("  order:\n")
        f.write("    proto: http\n")
        f.write("    addr: 5002\n")
        f.write("  shopping-cart-kafka:\n")
        f.write("    proto: tcp\n")
        f.write("    addr: 9092\n")
    
    print(f"Created {NGROK_CONFIG} with tunnels for order service and Kafka.")
    
    print("\nngrok setup complete. You can now start ngrok with:")
    print("./driver.py start-ngrok")

def extract_urls_from_log(log_content):
    """Extract URLs from ngrok log content."""
    order_service_url = None
    kafka_bootstrap_server = None
    
    https_matches = re.findall(r'name=order addr=http://localhost:5002 url=(https://[^\s]+)', log_content)
    if https_matches:
        order_service_url = https_matches[0]
    
    kafka_matches = re.findall(r'name=shopping-cart-kafka addr=//localhost:9092 url=(tcp://[^\s]+)', log_content)
    if kafka_matches:
        kafka_url = kafka_matches[0]
        kafka_bootstrap_server = kafka_url.replace('tcp://', '')
    
    return order_service_url, kafka_bootstrap_server

def update_env_file(env_vars, order_service_url=None, kafka_bootstrap_server=None):
    """Update .env file with new URLs."""
    if not (order_service_url or kafka_bootstrap_server):
        return
    
    with open(".env", "r") as f:
        lines = f.readlines()
    
    updated_lines = []
    order_found = kafka_found = False
    
    for line in lines:
        if line.strip().startswith("ORDER_SERVICE_URL=") and order_service_url:
            updated_lines.append(f'ORDER_SERVICE_URL="{order_service_url}"\n')
            order_found = True
        elif line.strip().startswith("KAFKA_BOOTSTRAP_SERVERS=") and kafka_bootstrap_server:
            updated_lines.append(f'KAFKA_BOOTSTRAP_SERVERS="{kafka_bootstrap_server}"\n')
            kafka_found = True
        else:
            updated_lines.append(line)
    
    if not order_found and order_service_url:
        updated_lines.append(f'ORDER_SERVICE_URL="{order_service_url}"\n')
    if not kafka_found and kafka_bootstrap_server:
        updated_lines.append(f'KAFKA_BOOTSTRAP_SERVERS="{kafka_bootstrap_server}"\n')
    
    with open(".env", "w") as f:
        f.writelines(updated_lines)

def start_ngrok(env_vars=None, debug=False):
    """Start ngrok using configuration file."""
    if not Path(NGROK_CONFIG).exists():
        print(f"Error: ngrok configuration file {NGROK_CONFIG} not found.")
        print("Please run './driver.py setup-ngrok' first.")
        sys.exit(1)

    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        log_file = temp_file.name

    try:
        process = subprocess.Popen(
            ["ngrok", "start", "--config", NGROK_CONFIG, "--all", "--log", log_file],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT
        )
        
        if debug:
            print(f"\nMonitoring ngrok log file: {log_file}")

        start_time = time.time()
        order_service_url = kafka_bootstrap_server = None
        
        while time.time() - start_time < 20 and (order_service_url is None or kafka_bootstrap_server is None):
            try:
                with open(log_file, 'r') as f:
                    log_content = f.read()
                    if debug:
                        print("\n[ngrok log file contents]:\n")
                        print(log_content)
                
                order_service_url, kafka_bootstrap_server = extract_urls_from_log(log_content)
                if order_service_url:
                    print(f"Found Order Service URL: {order_service_url}")
                if kafka_bootstrap_server:
                    print(f"Found Kafka Bootstrap Server: {kafka_bootstrap_server}")
                
                if not (order_service_url or kafka_bootstrap_server):
                    time.sleep(0.5)
            except Exception:
                time.sleep(0.5)
        
        if order_service_url or kafka_bootstrap_server:
            update_env_file(env_vars, order_service_url, kafka_bootstrap_server)
            print(f"\nUpdated {ENV_FILE} with ngrok URLs.")
        
        print("\nngrok is now running. Press Ctrl+C in this terminal to stop it.")
        process.wait()
    
    except KeyboardInterrupt:
        print("\nngrok interrupted by user.")
        if process and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
    except Exception as e:
        print(f"\nError running ngrok: {str(e)}")
        if process and process.poll() is None:
            process.terminate()
    finally:
        try:
            os.unlink(log_file)
        except Exception:
            pass