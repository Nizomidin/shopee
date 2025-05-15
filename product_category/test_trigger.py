import dotenv
import os
import argparse
import pika
from util.rmq import channel, connection

dotenv.load_dotenv()

# argument will be shop_id (string) and item_id (string)


def parse_args():
    parser = argparse.ArgumentParser(
        description='Publish a message to RabbitMQ.')
    parser.add_argument('--shop_id', type=str,
                        help='The shop ID to send', required=False)
    parser.add_argument('--item_id', type=str,
                        help='The item ID to send', required=False)
    return parser.parse_args()


def main():
    args = parse_args()

    # Declare the queue (must be the same)
    channel.queue_declare(queue=os.getenv(
        'PRODUCT_CATEGORY_QUEUE'), durable=True)

    # Publish a message
    if args.shop_id and args.item_id:
        message = f"{args.shop_id}_{args.item_id}"
    else:
        message = '{"id":2,"userId":"943|Jj68208fsTFYZn1Y3A0xUOoPfyCUI6cE0I74z9Cjd079ed01","token":"943|Jj68208fsTFYZn1Y3A0xUOoPfyCUI6cE0I74z9Cjd079ed01","data":{"url":"https://shopee.tw/---i.959032058.25627487646","priority":1},"type":"scraper"}'
    channel.basic_publish(exchange='', routing_key=os.getenv(
        'PRODUCT_CATEGORY_QUEUE'), body=message, properties=pika.BasicProperties(
            expiration=os.getenv('RMQ_MESSAGE_EXPIRY_MS', '1800000'),
    ))

    print(f" [x] Sent '{message}'")

    connection.close()


if __name__ == "__main__":
    main()
