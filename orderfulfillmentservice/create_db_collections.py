from pymongo import MongoClient
import os
from dotenv import load_dotenv
import pprint
from urllib.parse import quote_plus

load_dotenv()

# MongoDB connection setup
MONGO_URL = os.getenv("MONGO_URL")
MONGO_USER = os.getenv("MONGO_USER")
MONGO_PASS = os.getenv("MONGO_PASS")

# Define the collections that need change streams enabled
collections_config = [
    {"db": "shoppingcartdb", "collection": "shoppingcart"},
    {"db": "orderdb", "collection": "orders"},
    {"db": "orderdb", "collection": "fulfilled_orders"},
    {"db": "orderdb", "collection": "invalid_orders"},
    {"db": "shipmentdb", "collection": "delayed_orders"},
    {"db": "shipmentdb", "collection": "shipped_orders"},
    {
        "db": "shoppingcartdb",
        "collection": "incoming_shopping_cart_events",
        "capped": True,
        "size": 1000000,
    },
]


def get_mongodb_client():
    """Create and return a MongoDB client"""
    encoded_user = quote_plus(MONGO_USER)
    encoded_pass = quote_plus(MONGO_PASS)

    # Build MongoDB connection string with authentication
    auth_mongo_url = f"mongodb+srv://{encoded_user}:{encoded_pass}{MONGO_URL}"
    return MongoClient(auth_mongo_url, serverSelectionTimeoutMS=5000)


def create_database_and_collection(
    client, db_name, collection_name, is_capped_coll, size
):
    """Create a database and collection if they don't exist"""
    try:
        db = client[db_name]

        # Creating a collection
        print(f"\nCreating collection {db_name}.{collection_name}")
        if collection_name not in db.list_collection_names():
            if is_capped_coll:
                db.create_collection(collection_name, capped=True, size=size)
            else:
                db.create_collection(collection_name)
            print(f"Successfully created collection {db_name}.{collection_name}")
        else:
            print(f"Collection {db_name}.{collection_name} already exists")

        return True

    except Exception as e:
        print(
            f"Error creating database/collection {db_name}.{collection_name}: {str(e)}"
        )
        return False


def enable_change_streams_for_collection(client, db_name, collection_name):
    """Enable change streams with pre and post images for a specific collection"""
    try:
        db = client[db_name]

        print(f"\nEnabling change streams for {db_name}.{collection_name}")
        result = db.command(
            "collMod", collection_name, changeStreamPreAndPostImages={"enabled": True}
        )

        print(f"Successfully enabled change streams for {db_name}.{collection_name}")
        pprint.pprint(result)
        return True

    except Exception as e:
        print(
            f"Error enabling change streams for {db_name}.{collection_name}: {str(e)}"
        )
        return False


def main():
    client = get_mongodb_client()

    try:
        # Test the connection
        client.admin.command("ping")
        print("Successfully connected to MongoDB")

        # Create all databases and collections
        for config in collections_config:
            is_capped_coll = config.get("capped", False)

            if not create_database_and_collection(
                client,
                config["db"],
                config["collection"],
                is_capped_coll,
                config.get("size", 0),
            ):
                print(
                    f"Failed to create {config['db']}.{config['collection']}. Exiting."
                )
                return

        # Enable change streams
        success_count = 0
        total_collections = len(collections_config)

        for config in collections_config:
            if enable_change_streams_for_collection(
                client, config["db"], config["collection"]
            ):
                success_count += 1

        print(
            f"\nSummary: Successfully configured {success_count} out of {total_collections} collections"
        )

    except Exception as e:
        print(f"Error connecting to MongoDB: {str(e)}")

    finally:
        client.close()


if __name__ == "__main__":
    main()
