"""Command registry and command classes for the driver script."""
import subprocess
import sys

class Command:
    """Command class to encapsulate command functionality."""
    def __init__(self, name, func, label, needs_kafka=False, category="general"):
        self.name = name
        self.func = func
        self.label = label
        self.needs_kafka = needs_kafka
        self.category = category
    
    def execute(self, env_vars, extra_args=None, debug=False):
        try:
            if self.name == "get-order-history" and extra_args:
                self.func(env_vars, extra_args[0])
            elif self.name == "start-ngrok":
                self.func(env_vars, debug=debug)
            else:
                self.func(env_vars)
            return True
        except Exception as e:
            print(f"\nError executing command '{self.name}': {str(e)}")
            sys.exit(1)

class CommandRegistry:
    """Registry to manage all available commands."""
    def __init__(self):
        self.commands = {}
        self.aliases = {}
    
    def register(self, name, func, label, needs_kafka=False, category="setup"):
        self.commands[name] = Command(name, func, label, needs_kafka, category)
        return self
    
    def add_alias(self, alias, target):
        self.aliases[alias] = target
        return self
    
    def get_command(self, name):
        if name in self.aliases:
            name = self.aliases[name]
        
        cmd = self.commands.get(name)
        if not cmd:
            print(f"Unknown command: {name}")
            print("Available commands:")
            for c in self.commands:
                print(f"  {c}")
            sys.exit(1)
        
        return cmd
    
    def get_by_category(self):
        categories = {}
        for cmd in self.commands.values():
            if cmd.category not in categories:
                categories[cmd.category] = []
            categories[cmd.category].append(cmd)
        return categories
    
    def list_all(self):
        return list(self.commands.values())

def run_command(cmd_array, check=True):
    """Run a command and exit if it fails."""
    try:
        result = subprocess.run(cmd_array, check=check)
        return result
    except subprocess.CalledProcessError as e:
        print(f"\nError: Command failed with exit code {e.returncode}")
        print(f"Command: {' '.join(cmd_array)}")
        sys.exit(e.returncode)
    except KeyboardInterrupt:
        print("\nCommand interrupted by user.")
        sys.exit(130)  # Standard exit code for SIGINT