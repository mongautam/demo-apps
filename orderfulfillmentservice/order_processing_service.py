from flask import Flask, jsonify, request
import pprint
import random
import uuid
from pymongo import MongoClient
from dotenv import load_dotenv
from urllib.parse import quote_plus
from pymongo import MongoClient
import logging
import os
from constants import *

load_dotenv()


app = Flask(__name__)

MONGO_URL = os.getenv("MONGO_URL")

MONGO_USER = os.getenv("MONGO_USER")
MONGO_PASS = os.getenv("MONGO_PASS")

encoded_user = quote_plus(MONGO_USER)
encoded_pass = quote_plus(MONGO_PASS)

AUTH_MONGO_URL = f"mongodb+srv://{encoded_user}:{encoded_pass}{MONGO_URL}"
client = MongoClient(AUTH_MONGO_URL, serverSelectionTimeoutMS=5000)
db = client["orderhistorydb"]
order_history_collection = db["order_history"]


@app.route("/")
def hello():
    pprint.pprint("hello called")
    return jsonify({"message": "Hello World!"})


@app.route("/processOrder", methods=["POST"])
def process_order():
    order = request.get_json()
    pprint.pprint(order)

    validated_order = {
        "_id": order["fullDocument"]["order_id"],
        "order_id": order["fullDocument"]["order_id"],
        "cart_id": order["fullDocument"]["cart_id"],
        "status": "order_fulfilled" if random.randint(1, 10) < 8 else "order_invalid",
        "items": order["fullDocument"]["items"],
    }
    pprint.pprint(validated_order)
    return jsonify({"message": validated_order})


@app.route("/shipOrder", methods=["POST"])
def ship_order():
    order = request.get_json()
    pprint.pprint(order)
    shipment_id = str(uuid.uuid4())
    shipped_order = {
        "_id": shipment_id,
        "order_id": order["fullDocument"]["order_id"],
        "shipment_id": shipment_id,
        "status": "order_shipped" if random.randint(1, 10) < 8 else "order_delayed",
        "items": order["fullDocument"]["items"],
    }
    pprint.pprint(shipped_order)
    return jsonify({"message": shipped_order})


@app.route("/getOrderHistory", methods=["GET"])
def getOrderHistory():
    pprint.pprint("getOrderHistory called")

    order_id = request.args.get("orderId")
    pprint.pprint(f"orderId = {order_id}")
    if not order_id:
        return jsonify({"error": "Missing orderId parameter"}), 400

    order = order_history_collection.find_one({"_id": order_id})
    if not order:
        return jsonify({"error": "Order not found"}), 404

    # Convert ObjectId to string for JSON serialization
    order["_id"] = str(order["_id"])
    return jsonify(order)


if __name__ == "__main__":
    app.run(port=5002)
