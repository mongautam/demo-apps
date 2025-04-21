import requests
from dotenv import load_dotenv
import os
import argparse
import json


def main():
    load_dotenv()

    parser = argparse.ArgumentParser(description="Get order history by order ID")
    parser.add_argument("order_id", help="The order ID to look up")
    args = parser.parse_args()

    ORDER_SERVICE_URL = os.environ["ORDER_SERVICE_URL"]
    response = requests.get(
        f"{ORDER_SERVICE_URL}/getOrderHistory?orderId={args.order_id}",
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
