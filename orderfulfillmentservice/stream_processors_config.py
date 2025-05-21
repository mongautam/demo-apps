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
        "name": "delayedShipmentToOrderTrackingStreamProcessor",
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
                    "items": "$fullDocument.items",
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
]

kafka_stream_processor = {
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
}
