import os
from dotenv import load_dotenv
from urllib.parse import quote_plus
from pymongo import MongoClient
import logging
import random
import uuid
import time
import json
import argparse
from constants import *

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Load environment variables
load_dotenv()


class EventDestination:
    def __init__(self, destination="mongodb"):
        if destination == "mongodb":
            self.setup_mongodb()
        elif destination == "kafka":
            # Only import kafka when needed
            from kafka import KafkaProducer

            self.KafkaProducer = KafkaProducer
            self.setup_kafka()

    def setup_mongodb(self):
        """Setup MongoDB connection"""
        MONGO_URL = os.getenv("MONGO_URL")
        MONGO_DB = os.getenv("SHOPPING_CART_DB_NAME", "shoppingcartdb")
        MONGO_COLLECTION = (
            "incoming_shopping_cart_events"  # Using the capped collection
        )
        MONGO_USER = os.getenv("MONGO_USER")
        MONGO_PASS = os.getenv("MONGO_PASS")

        encoded_user = quote_plus(MONGO_USER)
        encoded_pass = quote_plus(MONGO_PASS)

        # Build MongoDB connection string with authentication
        AUTH_MONGO_URL = f"mongodb+srv://{encoded_user}:{encoded_pass}{MONGO_URL}"
        self.mongo_client = MongoClient(AUTH_MONGO_URL, serverSelectionTimeoutMS=5000)
        self.db = self.mongo_client[MONGO_DB]
        self.collection = self.db[MONGO_COLLECTION]
        logging.info("Connected to MongoDB successfully.")

    def setup_kafka(self):
        """Setup Kafka producer"""
        KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        KAFKA_TOPIC = os.getenv("KAFKA_SHOPPING_CART_TOPIC", "shopping-cart-events")
        self.kafka_username = os.getenv("KAFKA_USERNAME", "admin")
        self.kafka_password = os.getenv("KAFKA_PASSWORD", "admin-secret")
        self.kafka_topic = KAFKA_TOPIC
        self.producer = self.KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            security_protocol="SASL_PLAINTEXT",
            sasl_mechanism="PLAIN",
            sasl_plain_username=self.kafka_username,
            sasl_plain_password=self.kafka_password,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda v: str(v).encode("utf-8"),
        )
        logging.info("Connected to Kafka successfully.")

    def send_event(self, event, destination):
        """Send event to specified destination"""
        if destination == "mongodb":
            event_doc = {
                "_id": str(uuid.uuid4()),  # Unique ID for the event
                "timestamp": int(time.time() * 1000),
                "event_type": event.get(STATUS),
                "cart_data": event,
            }
            self.collection.insert_one(event_doc)
            logging.info(
                f"Inserted event into capped collection: {event_doc['event_type']}"
            )

        elif destination == "kafka":
            key = str(event.get("customer_id", "default"))
            self.producer.send(self.kafka_topic, key=key, value=event)
            self.producer.flush()

    def close(self):
        """Close all connections"""
        if hasattr(self, "mongo_client"):
            self.mongo_client.close()
        if hasattr(self, "producer"):
            self.producer.close()


def generate_cart_events(destination_handler, destination):
    open_shopping_carts = {}

    # Generate events for multiple customers
    for _ in range(100):
        customer_id = random.randint(1, 25)

        # If customer doesn't have a cart, create one with initial items
        if customer_id not in open_shopping_carts:
            cart = {
                "_id": str(uuid.uuid4()),
                "customer_id": customer_id,
                "items": [random.randint(1, 100)],
                STATUS: CREATE_SHOPPING_CART,
                "timestamp": int(time.time() * 1000),
            }
            open_shopping_carts[customer_id] = cart
            destination_handler.send_event(cart, destination)
            logging.info(f"Created new cart event for customer {customer_id}: {cart}")
            time.sleep(0.5)

        # For existing carts, either add items or convert to order
        else:
            cart = open_shopping_carts[customer_id]

            # 70% chance to add item, 30% chance to create order
            if random.random() < 0.7:
                # Add item to cart
                cart["items"].append(random.randint(1, 100))
                cart["timestamp"] = int(time.time() * 1000)
                cart[STATUS] = UPDATE_SHOPPING_CART  # New status for updates
                destination_handler.send_event(cart, destination)
                logging.info(
                    f"Created update cart event for customer {customer_id}: {cart}"
                )
            else:
                # Convert cart to order
                cart[STATUS] = CREATE_ORDER
                cart["order_id"] = str(uuid.uuid4())
                cart["timestamp"] = int(time.time() * 1000)
                destination_handler.send_event(cart, destination)
                logging.info(
                    f"Created order event from cart for customer {customer_id}: {cart}"
                )
                del open_shopping_carts[customer_id]

            time.sleep(0.5)

    # Convert any remaining carts to orders
    for customer_id, cart in open_shopping_carts.copy().items():
        cart[STATUS] = CREATE_ORDER
        cart["order_id"] = str(uuid.uuid4())
        cart["timestamp"] = int(time.time() * 1000)
        destination_handler.send_event(cart, destination)
        logging.info(
            f"Created order event from remaining cart for customer {customer_id}: {cart}"
        )
        del open_shopping_carts[customer_id]
        time.sleep(0.5)


def main():
    parser = argparse.ArgumentParser(description="Generate shopping cart events")
    parser.add_argument(
        "--destination",
        choices=["mongodb", "kafka"],
        default="mongodb",
        help="Destination for events (mongodb or kafka, defaults to mongodb)",
    )
    args = parser.parse_args()

    destination_handler = EventDestination(args.destination)

    logging.info(
        f"Starting shopping cart event generator... (Destination: {args.destination})"
    )

    try:
        while True:
            try:
                generate_cart_events(destination_handler, args.destination)
                logging.info(
                    "Completed one round of event generation. Starting next round..."
                )
                time.sleep(2)  # Wait 2 seconds between rounds
            except KeyboardInterrupt:
                logging.info("Shutting down event generator...")
                break
            except Exception as e:
                logging.error(f"Error generating events: {e}")
                time.sleep(5)  # Wait longer if there's an error
    finally:
        destination_handler.close()


if __name__ == "__main__":
    main()
