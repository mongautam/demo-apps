#!/usr/bin/env python3
"""
Driver script for Order Fulfillment Demo application.
Provides a simple CLI and interactive menu for running demo components.
"""
import sys
import argparse
from pathlib import Path

from src.commands import CommandRegistry, run_command
from src.env_setup import setup_environment, prompt_for_env_vars
from src.ngrok_utils import setup_ngrok, start_ngrok
from src.menu import print_menu, print_simulation_info

NGROK_COMMANDS = ["setup-ngrok", "start-ngrok"]

def start_order_service(env_vars=None):
    """Start the local order processing service."""
    run_command([sys.executable, "order_processing_service.py"])

def simulate_shopping(env_vars, use_kafka=False):
    """Simulate customers adding items to carts and checking out."""
    if use_kafka:
        prompt_for_env_vars(env_vars, only_kafka=True)
        destination = "kafka"
    else:
        destination = "mongodb"
    
    print_simulation_info()
    run_command([sys.executable, "shopping_cart_event_generator.py", "--destination", destination])

def setup_all():
    """Run all setup steps in sequence."""
    print("Setting up database and collections...")
    run_command([sys.executable, "create_db_collections.py"])
    
    print("\nCreating stream processor instance...")
    run_command([sys.executable, "create_stream_processor_instance.py"])
    
    print("\nSetting up stream processor connections...")
    run_command([sys.executable, "create_stream_processor_connections.py"])
    
    print("\nSetting up stream processors...")
    run_command([sys.executable, "create_stream_processors.py"])
    
    print("\nSetup complete! You can now start the stream processors with:")
    print("./driver.py start-stream-processors")
    print("Then run the shopping cart simulator with:")
    print("./driver.py simulate-shopping")

def get_order_history(order_id=None):
    """Get history for a specific order."""
    if order_id:
        run_command([sys.executable, "get_order_history.py", order_id])
    else:
        order_id = input("Enter Order ID: ")
        run_command([sys.executable, "get_order_history.py", order_id])

def start_stream_processors(use_kafka=False):
    """Start all stream processors."""
    command = [sys.executable, "start_stream_processors.py"]
    if use_kafka:
        command.append("--kafka")
    run_command(command)

def initialize_registry():
    """Initialize the command registry with all available commands."""
    registry = CommandRegistry()
    
    # Setup commands
    registry.register("setup-ngrok", setup_ngrok, 
                     "Configure ngrok for local development", category="setup")
    registry.register("start-ngrok", start_ngrok, 
                     "Start ngrok tunnels", category="setup")
    registry.register("start-order-service", start_order_service, 
                     "Start Order Processing Service", category="setup", needs_kafka=False)
    registry.register("setup-database", 
                     lambda env: run_command([sys.executable, "create_db_collections.py"]), 
                     "Setup Database and Collections", category="setup")
    registry.register("create-stream-processor-instance", 
                     lambda env: run_command([sys.executable, "create_stream_processor_instance.py"]), 
                     "Create Stream Processor Instance", category="setup")
    registry.register("setup-stream-processor-connections", 
                     lambda env: run_command([sys.executable, "create_stream_processor_connections.py"]), 
                     "Setup Stream Processor Connections", category="setup")
    registry.register("setup-stream-processors", 
                     lambda env: run_command([sys.executable, "create_stream_processors.py"]), 
                     "Setup and Start Stream Processors", category="setup")
    registry.register("setup-all", setup_all, 
                     "Run All Setup Steps", category="setup")
    registry.register("start-stream-processors", 
                     start_stream_processors(use_kafka=False), 
                     "Start all stream processors", category="setup")
    registry.register("start-stream-processors-kafka", 
                     start_stream_processors(use_kafka=True), 
                     "Start all stream processors including Kafka", category="setup")
    
    # Simulation commands
    registry.register("simulate-shopping", 
                     lambda env: simulate_shopping(env, use_kafka=False), 
                     "Simulate Customer Shopping Activity", 
                     needs_kafka=False, category="simulation")
    registry.register("simulate-shopping-kafka", 
                     lambda env: simulate_shopping(env, use_kafka=True), 
                     "Simulate Customer Shopping (via Kafka)", 
                     needs_kafka=True, category="simulation")
    
    # Utility commands
    registry.register("get-order-history", 
                     lambda extra_args: get_order_history(order_id=extra_args if extra_args and len(extra_args) > 0 else None), 
                     "Retrieve Order History for an Order", 
                     category="utility")
    
    return registry

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('command', nargs='?', help='Command to execute')
    parser.add_argument('extra_args', nargs=argparse.REMAINDER, help='Additional arguments for the command')
    parser.add_argument('--debug', action='store_true', help='Print debug information for ngrok commands')
    args = parser.parse_args()
    cmd = args.command
    extra_args = args.extra_args
    debug = args.debug

    env_vars = setup_environment()
    registry = initialize_registry()

    if cmd:
        cmd_obj = registry.get_command(cmd)

        if cmd in NGROK_COMMANDS:
            pass  # No env var checks for ngrok commands
        elif cmd_obj.needs_kafka:
            prompt_for_env_vars(env_vars, only_kafka=True)
        else:
            prompt_for_env_vars(env_vars, only_kafka=False)

        if cmd == 'start-ngrok':
            cmd_obj.execute(env_vars, extra_args=None, debug=debug)
        else:
            cmd_obj.execute(env_vars, extra_args)
        sys.exit(0)

    # Interactive menu
    else:
        while True:
            try:
                indexed_commands = print_menu(registry)
                choice = input("Enter your choice: ").strip()

                if choice == "0":
                    print("Exiting.")
                    break

                try:
                    idx = int(choice) - 1
                    if 0 <= idx < len(indexed_commands):
                        cmd = indexed_commands[idx]
                        if cmd.name in NGROK_COMMANDS:
                            pass
                        elif cmd.needs_kafka:
                            prompt_for_env_vars(env_vars, only_kafka=True)
                        else:
                            prompt_for_env_vars(env_vars, only_kafka=False)
                        cmd.execute(env_vars)
                    else:
                        print("Invalid choice.")
                except ValueError:
                    print("Invalid choice.")
            except KeyboardInterrupt:
                print("\nExiting.")
                break

if __name__ == "__main__":
    main()