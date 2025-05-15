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
    parser.add_argument('--task_id', type=str,
                        help='The task ID to send', required=False)
    return parser.parse_args()


def main():
    args = parse_args()

    # Declare the queue (must be the same)
    channel.queue_declare(queue=os.getenv(
        'PRODUCT_CATEGORY_QUEUE'), durable=True)

    # Publish a message
    if args.task_id:
        message = args.task_id
    else:
        message = "results/12312_15902082401/2025-04-29T21:56:36.749Z.json"
    channel.basic_publish(exchange='', routing_key=os.getenv(
        'PROFILE_QUEUE'), body=message, properties=pika.BasicProperties(
            expiration=os.getenv('RMQ_MESSAGE_EXPIRY_MS', '1800000'),
    ))

    print(f" [x] Sent '{message}'")

    connection.close()


if __name__ == "__main__":
    main()
