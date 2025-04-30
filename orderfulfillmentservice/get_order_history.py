import requests
from dotenv import load_dotenv
import os
import argparse
import json


def main():
    load_dotenv()

    parser = argparse.ArgumentParser(description="Get order history by order ID")
    parser.add_argument("order_id", nargs="?", help="The order ID to look up (optional)")
    args = parser.parse_args()

    order_id = args.order_id

    ORDER_SERVICE_URL = os.environ["ORDER_SERVICE_URL"]
    if order_id:
        url = f"{ORDER_SERVICE_URL}/getOrderHistory?orderId={order_id}"
    else:
        url = f"{ORDER_SERVICE_URL}/getOrderHistory"
    response = requests.get(
        url,
        headers={
            "ngrok-skip-browser-warning": "true",
        },
    )

    if response.status_code == 200:
        print(json.dumps(response.json(), indent=2))
    else:
        print(f"Error: {response.status_code}")
        print(response.text)


if __name__ == "__main__":
    main()
