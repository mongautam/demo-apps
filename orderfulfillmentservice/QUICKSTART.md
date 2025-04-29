# Real-time Order Fulfillment Demo - Quick Start

## What is this demo?

This demo shows a real-time order fulfillment system using MongoDB Atlas Stream Processor (ASP) that connects:

- A simulated shopping cart service generating events
- A local order processing service (Flask)
- MongoDB Atlas for order history storage

The system uses an event-driven architecture where:

1. Shopping cart events are generated
2. Atlas Stream Processor listens for these events
3. When cart items are ready to order, ASP calls the order service
4. Order service processes the order and records status in MongoDB
5. You can query the order history to see the full lifecycle

## Quick Start Steps

### Prerequisites:

First, make sure you have:

1. Clone this repository:
   ```sh
    git clone [https://github.com/mongautam/demo-apps.git](https://github.com/mongautam/demo-apps.git)
    cd demo-apps/atlas-stream-processing/order-fulfillment-demo # Adjust path as needed
   ```

2. Create and activate a Python virtual environment:
   ```sh
   python3 -m venv venv
   source venv/bin/activate  # On macOS/Linux
   # OR
   venv\Scripts\activate     # On Windows
   ```

3. Install the required dependencies:
   ```sh
   pip install -r requirements.txt
   ```

4. Configured ngrok (see the README.md for details)

### Quickest path to running the demo:

1. In terminal 1: `./driver.py setup-ngrok` (run once to configure ngrok)
2. In terminal 1: `./driver.py start-ngrok`
3. In terminal 2: `./driver.py start-order-service`
4. In terminal 3: `./driver.py setup-all && ./driver.py start-stream-processors && ./driver.py simulate-shopping`

### Detailed setup steps:

Follow these steps to run the demo using driver.py:

1. Configure your environment variables:

   - Edit the `.env` file and fill in all required values. If `.env` does not exist, it will be created from the `env` template when you run driver.py.

2. Create and activate a Python virtual environment (recommended):

   ```sh
   python3 -m venv venv
   source venv/bin/activate  # On macOS/Linux
   # venv\Scripts\activate  # On Windows
   ```

3. Install dependencies (required before running driver.py):

   ```sh
   pip install -r requirements.txt
   ```

   The script will check for required dependencies and will not run without them.

4. Set up ngrok configuration (run once):

   ```sh
   ./driver.py setup-ngrok
   ```

   - This will prompt you for your ngrok auth token and create a local ngrok config file with the required tunnels.

5. Use driver.py to run each step of the demo:

   a. Start ngrok (in a separate terminal):

   ```sh
   ./driver.py start-ngrok
   ```

   - This will start ngrok using your local config and update your `.env` with the ngrok URLs for ORDER_SERVICE_URL and KAFKA_BOOTSTRAP_SERVERS after ngrok starts.

   b. Start the local order processing service (in a new terminal):

   ```sh
   ./driver.py start-order-service
   ```

   c. Set up the database and collections:

   ```sh
   ./driver.py setup-database
   ```

   d. Create the stream processor instance:

   ```sh
   ./driver.py create-stream-processor-instance
   ```

   e. Set up stream processor connections:

   ```sh
   ./driver.py setup-stream-processor-connections
   ```

   f. Set up and start stream processors:

   ```sh
   ./driver.py setup-stream-processors
   ```

   g. Run the shopping cart simulator:

   ```sh
   ./driver.py simulate-shopping
   # Or for the alternate method (requires additional configuration):
   # ./driver.py simulate-shopping-alt
   ```

   h. Retrieve order history for a specific order ID:

   ```sh
   ./driver.py get-order-history <order_id>
   ```

You can also run `./driver.py` with no arguments to use the interactive menu.

## Requirements

- MongoDB Atlas account (M0 or higher cluster)
- Python 3.12 or later
- ngrok account and CLI tool
- Python dependencies installed via `pip install -r requirements.txt`
- (Optional) Local Kafka setup for Kafka event source

For full documentation, see the [README.md](README.md) file.
