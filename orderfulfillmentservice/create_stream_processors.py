import pprint
from requests.auth import HTTPDigestAuth
import requests
import os
import sys
from constants import *
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env file

PUBLIC_KEY = os.environ["ATLAS_API_PUBLIC_KEY"]
PRIVATE_KEY = os.environ["ATLAS_API_PRIVATE_KEY"]
PROJECT_ID = os.environ["ATLAS_PROJECT_ID"]
STREAM_INSTANCE_NAME = os.environ["STREAM_PROCESSOR_INSTANCE_NAME"]
API_URL = f"https://cloud.mongodb.com/api/atlas/v2/groups/{PROJECT_ID}/streams/{STREAM_INSTANCE_NAME}/processor"

headers = {
    "Accept": "application/vnd.atlas.2024-05-30+json",
    "Content-Type": "application/json",
}

# Define all stream processors
stream_processors = [
    {
        "name": "shoppingCartToOrderStreamProcessor",
        "pipeline": [
            {
                "$source": {
                    "connectionName": "mongoDBSink",
                    "db": "shoppingcartdb",
                    "coll": "shoppingcart",
                    "config": {"fullDocument": "whenAvailable"},
                }
            },
            {
                "$match": {
                    "operationType": "replace",
                    "fullDocument.status": "create_order",
                }
            },
            {
                "$project": {
                    "_id": "$fullDocument.order_id",
                    "cart_id": "$fullDocument._id",
                    "order_id": "$fullDocument.order_id",
                    "status": "order_created",
                    "items": "$fullDocument.items",
                }
            },
            {
                "$merge": {
                    "into": {
                        "connectionName": "mongoDBSink",
                        "db": "orderdb",
                        "coll": "orders",
                    },
                    "whenMatched": "replace",
                    "whenNotMatched": "insert",
                }
            },
        ],
    },
    {
        "name": "orderValidationStreamProcessor",
        "pipeline": [
            {
                "$source": {
                    "connectionName": "mongoDBSink",
                    "db": "orderdb",
                    "coll": "orders",
                    "config": {"fullDocument": "whenAvailable"},
                }
            },
            {
                "$match": {
                    "operationType": "insert",
                    "fullDocument.status": "order_created",
                }
            },
            {
                "$https": {
                    "connectionName": "flaskService",
                    "path": "/processOrder",
                    "method": "POST",
                    "as": "message",
                }
            },
            {
                "$project": {
                    "_id": "$message.message.order_id",
                    "order_id": "$message.message.order_id",
                    "cart_id": "$message.message.cart_id",
                    "status": "$message.message.status",
                    "items": "$message.message.items",
                }
            },
            {
                "$addFields": {
                    "destination_collection": {
                        "$cond": {
                            "if": {"$eq": ["$status", "order_fulfilled"]},
                            "then": "fulfilled_orders",
                            "else": "invalid_orders",
                        }
                    }
                }
            },
            {
                "$merge": {
                    "into": {
                        "connectionName": "mongoDBSink",
                        "db": "orderdb",
                        "coll": "$destination_collection",
                    }
                }
            },
        ],
    },
    {
        "name": "orderToShipmentStreamProcessor",
        "options": {
            "dlq": {
                "connectionName": "mongoDBSink",
                "db": "dlqDb",
                "coll": "dlqColl",
            }
        },
        "pipeline": [
            {
                "$source": {
                    "connectionName": "mongoDBSink",
                    "db": "orderdb",
                    "coll": "fulfilled_orders",
                    "config": {"fullDocument": "whenAvailable"},
                }
            },
            {
                "$match": {
                    "operationType": "insert",
                }
            },
            {
                "$https": {
                    "connectionName": "flaskService",
                    "path": "/shipOrder",
                    "method": "POST",
                    "as": "message",
                }
            },
            {
                "$project": {
                    "_id": "$message.message.shipment_id",
                    "order_id": "$message.message.order_id",
                    "status": "$message.message.status",
                    "items": "$message.message.items",
                }
            },
            {
                "$addFields": {
                    "destination_collection": {
                        "$cond": {
                            "if": {"$eq": ["$status", "order_shipped"]},
                            "then": "shipped_orders",
                            "else": "delayed_orders",
                        }
                    }
                }
            },
            {
                "$merge": {
                    "into": {
                        "connectionName": "mongoDBSink",
                        "db": "shipmentdb",
                        "coll": "$destination_collection",
                    }
                }
            },
        ],
    },
    {
        "name": "shoppingCartToOrderTrackingStreamProcessor",
        "pipeline": [
            {
                "$source": {
                    "connectionName": "mongoDBSink",
                    "db": "shoppingcartdb",
                    "config": {"fullDocument": "whenAvailable"},
                }
            },
            {
                "$match": {
                    "operationType": "replace",
                    "fullDocument.status": "create_order",
                }
            },
            {
                "$project": {
                    "_id": "$fullDocument.order_id",
                    "cart_id": "$fullDocument._id",
                    "order_id": "$fullDocument.order_id",
                    "status": "order_created",
                    "items": "$fullDocument.items",
                    "customer_id": "$fullDocument.customer_id",
                }
            },
            {
                "$addFields": {
                    "create_order_status_event": {
                        "status": "$status",
                        "cart_id": "$cart_id",
                    }
                },
            },
            {
                "$unset": ["order_id", "status", "cart_id", "items"],
            },
            {
                "$merge": {
                    "into": {
                        "connectionName": "mongoDBSink",
                        "db": "orderhistorydb",
                        "coll": "order_history",
                    },
                    "whenMatched": "replace",
                    "whenNotMatched": "insert",
                }
            },
        ],
    },
    {
        "name": "fulfilledOrderToOrderTrackingStreamProcessor",
        "pipeline": [
            {
                "$source": {
                    "connectionName": "mongoDBSink",
                    "db": "orderdb",
                    "coll": "fulfilled_orders",
                    "config": {"fullDocument": "whenAvailable"},
                }
            },
            {
                "$match": {
                    "operationType": "insert",
                }
            },
            {
                "$project": {
                    "_id": "$fullDocument._id",
                    "cart_id": "$fullDocument.cart_id",
                    "items": "$fullDocument.items",
                    "source_collection": "$fullDocument.destination_collection",
                }
            },
            {
                "$addFields": {
                    "create_fulfilled_order_status_event": {
                        "status": "order_fulfilled",
                        "cart_id": "$cart_id",
                    },
                },
            },
            {
                "$merge": {
                    "into": {
                        "connectionName": "mongoDBSink",
                        "db": "orderhistorydb",
                        "coll": "order_history",
                    },
                    "on": "_id",
                }
            },
        ],
    },
    {
        "name": "invalidOrderToOrderTrackingStreamProcessor",
        "pipeline": [
            {
                "$source": {
                    "connectionName": "mongoDBSink",
                    "db": "orderdb",
                    "coll": "invalid_orders",
                    "config": {"fullDocument": "whenAvailable"},
                }
            },
            {
                "$match": {
                    "operationType": "insert",
                }
            },
            {
                "$project": {
                    "_id": "$fullDocument._id",
                    "cart_id": "$fullDocument.cart_id",
                    "items": "$fullDocument.items",
                    "source_collection": "$fullDocument.destination_collection",
                }
            },
            {
                "$addFields": {
                    "create_invalid_order_status_event": {
                        "status": "order_invalid",
                        "cart_id": "$fullDocument.cart_id",
                    },
                },
            },
            {
                "$merge": {
                    "into": {
                        "connectionName": "mongoDBSink",
                        "db": "orderhistorydb",
                        "coll": "order_history",
                    },
                    "on": "_id",
                }
            },
        ],
    },
    {
        "name": "shippedOrderToOrderTrackingStreamProcessor",
        "pipeline": [
            {
                "$source": {
                    "connectionName": "mongoDBSink",
                    "db": "shipmentdb",
                    "coll": "shipped_orders",
                    "config": {"fullDocument": "whenAvailable"},
                }
            },
            {
                "$match": {
                    "operationType": "insert",
                }
            },
            {
                "$project": {
                    "_id": "$fullDocument.order_id",
                    "items": "$fullDocument.items",
                    "source_collection": "$fullDocument.destination_collection",
                }
            },
            {
                "$addFields": {
                    "create_shipped_order_status_event": {
                        "status": "order_shipped",
                        "shipment_id": "$fullDocument._id",
                    },
                },
            },
            {
                "$merge": {
                    "into": {
                        "connectionName": "mongoDBSink",
                        "db": "orderhistorydb",
                        "coll": "order_history",
                    },
                    "on": "_id",
                }
            },
        ],
    },
    {
        "name": "delayedShipmentToOrderTrackingStreamProcessor",  # Name of your stream processor instance
        "options": {
            "dlq": {
                "connectionName": "mongoDBSink",
                "db": "dlqDb",
                "coll": "dlqColl",
            }
        },
        "pipeline": [
            {
                "$source": {
                    "connectionName": "mongoDBSink",
                    "db": "shipmentdb",
                    "coll": "delayed_orders",
                    "config": {"fullDocument": "whenAvailable"},
                }
            },
            {
                "$match": {
                    "operationType": "insert",
                }
            },
            {
                "$project": {
                    "_id": "$fullDocument.order_id",
                    "items": "$fullDocument.items",  # Assuming cart_id is in fullDocument
                    "source_collection": "$fullDocument.destination_collection",
                }
            },
            {
                "$addFields": {
                    "create_delayed_shipped_order_status_event": {
                        "status": "order_shipment_delayed",
                        "delayed_shipment_id": "$fullDocument._id",
                    },
                },
            },
            {
                "$merge": {
                    "into": {
                        "connectionName": "mongoDBSink",
                        "db": "orderhistorydb",
                        "coll": "order_history",
                    },
                    "on": "_id",
                }
            },
        ],
    },
    {
        "name": "shoppingCartEventsCappedCollectionToShoppingCartStreamProcessor",
        "pipeline": [
            {
                "$source": {
                    "connectionName": "mongoDBSink",
                    "db": "shoppingcartdb",
                    "coll": "incoming_shopping_cart_events",
                    "config": {"fullDocument": "whenAvailable"},
                }
            },
            {
                "$match": {
                    "operationType": "insert",
                }
            },
            {
                "$project": {
                    "_id": "$fullDocument.cart_data._id",
                    "cart_id": "$fullDocument.cart_data._id",
                    "status": "$fullDocument.cart_data.status",
                    "items": "$fullDocument.cart_data.items",
                    "customer_id": "$fullDocument.cart_data.customer_id",
                    "timestamp": "$fullDocument.timestamp",
                    "order_id": "$fullDocument.cart_data.order_id",
                }
            },
            {
                "$merge": {
                    "into": {
                        "connectionName": "mongoDBSink",
                        "db": "shoppingcartdb",
                        "coll": "shoppingcart",
                    },
                    "whenMatched": "replace",
                    "whenNotMatched": "insert",
                }
            },
        ],
    },
    {
        "name": "shoppingCartEventsFromKafkaStreamProcessor",
        "pipeline": [
            {
                "$source": {
                    "connectionName": "shoppingCartKafkaEventSource",
                    "topic": "shopping-cart-events",
                }
            },
            {
                "$project": {
                    "_id": "$_id",
                    "cart_id": "$_id",
                    "status": "$status",
                    "items": "$items",
                    "customer_id": "$customer_id",
                    "timestamp": "$timestamp",
                    "order_id": "$order_id",
                }
            },
            {
                "$merge": {
                    "into": {
                        "connectionName": "mongoDBSink",
                        "db": "shoppingcartdb",
                        "coll": "shoppingcart",
                    },
                    "whenMatched": "replace",
                    "whenNotMatched": "insert",
                }
            },
        ],
    },
]

# Create each stream processor and print the response
for processor in stream_processors:
    print(f"\nCreating stream processor: {processor['name']}")
    response = requests.post(
        API_URL,
        auth=HTTPDigestAuth(PUBLIC_KEY, PRIVATE_KEY),
        headers=headers,
        json=processor,
    )
    if not (200 <= response.status_code < 300):
        print(f"Error creating stream processor {processor['name']}:")
        pprint.pprint(response.json())
        sys.exit(1)
    pprint.pprint(response.json())
