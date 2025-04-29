"""Menu and simulation info display functions."""

def print_menu(registry):
    """Display interactive menu of available commands."""
    print("\nSelect an action:")
    
    categories = registry.get_by_category()
    start_idx = 1
    indexed_commands = []
    
    category_display = {
        "setup": "Setup",
        "simulation": "Simulation",
        "utility": "Utilities"
    }
    
    for category in ["setup", "simulation", "utility"]:
        if category in categories:
            print(f"\n== {category_display.get(category, category.capitalize())} ==")
            commands = categories[category]
            for i, cmd in enumerate(commands, start=start_idx):
                print(f"{i}. {cmd.label}")
                indexed_commands.append(cmd)
            start_idx += len(commands)
    
    print("\n0. Exit")
    return indexed_commands

def print_simulation_info():
    """Print information about what happens during the shopping simulation."""
    print("\n" + "="*80)
    print("Starting Shopping Simulation")
    print("="*80)
    print("""
What will happen:
1. Customers will add items to shopping carts
2. The Stream Processor will detect when carts are ready for checkout
3. Order Processing Service will receive order requests and process them
4. You'll see order processing logs in the Order Service terminal
5. All events will be recorded in the Order History database

Use Ctrl+C to stop the simulation when you've seen enough events
""")