import pika
import dotenv
import os

dotenv.load_dotenv()


# Connect to RabbitMQ server
connection = pika.BlockingConnection(
    pika.ConnectionParameters(os.getenv('RABBITMQ_HOST')))
channel = connection.channel()


def callback(ch, method, properties, body):
    print(f" [x] Received {body.decode()}")

    # TODO: implement the logic to process the message and publish to the next queue

    # publish the result if successful
    # or send another queue to profile service to retry


def main():

    # Declare the queue
    channel.queue_declare(queue=os.getenv('USER_BEHAVIOR_QUEUE'), durable=True)

    # Subscribe to the queue
    channel.basic_consume(
        queue=os.getenv('USER_BEHAVIOR_QUEUE'), on_message_callback=callback, auto_ack=True)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()


if __name__ == "__main__":
    main()
