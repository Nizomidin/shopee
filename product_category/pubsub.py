import os
import pika
import requests
import json
from util.rmq import channel
from util.logger import logger_product_category_service as logger
import dotenv

dotenv.load_dotenv()


def callback(ch, method, properties, body):
    query = json.loads(body.decode())
    logger.info(f" [x] Received {query}")

    # Prepare the payload
    payload = {
        "query": query
    }

    # Make the POST request
    response = requests.post(
        os.getenv('PRODUCT_CATEGORY_LAMBDA_URL'),
        data=json.dumps(payload),
        headers={"Content-Type": "application/json"}
    )

    # Check the response
    if response.status_code == 200:
        data = response.json()
        logger.info(f" [x] Successfully processed: {data}")

        # Declare the next queue
        channel.queue_declare(queue=os.getenv('PROFILE_QUEUE'), durable=True)
        body = json.loads(data['body']) if isinstance(
            data['body'], str) else data['body']
        message = body['s3Key']

        # Publish the result to the next queue
        channel.basic_publish(exchange='',  routing_key=os.getenv(
            'PROFILE_QUEUE'), body=message, properties=pika.BasicProperties(
            expiration=os.getenv('RMQ_MESSAGE_EXPIRY_MS', '1800000'),
        ))

        logger.info(
            f" [x] Sent to '{os.getenv('PROFILE_QUEUE')}': '{message}'")
    else:
        logger.error(
            f" [!] Failed to process: {response.status_code}, {response.text}")


def main():

    # Declare the queue
    queue_name = os.getenv('PRODUCT_CATEGORY_QUEUE')
    channel.queue_declare(queue=queue_name, durable=True)

    # Subscribe to the queue
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(
        queue=queue_name, on_message_callback=callback, auto_ack=True)

    logger.info(
        f' [*] Waiting for messages from "{queue_name}". To exit press CTRL+C')
    channel.start_consuming()


if __name__ == "__main__":
    main()
