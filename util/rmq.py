import pika
import dotenv
import os

dotenv.load_dotenv()

# Connect to RabbitMQ server
credentials = pika.PlainCredentials(
    os.getenv('RMQ_USERNAME'), os.getenv('RMQ_PASSWORD'))
connection = pika.BlockingConnection(
    pika.ConnectionParameters(
        host=os.getenv('RMQ_HOSTNAME'),
        port=int(os.getenv('RMQ_PORT')),
        credentials=credentials
    )
)
channel = connection.channel()
