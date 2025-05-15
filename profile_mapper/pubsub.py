import dotenv
import os
import json
import random
import boto3
import pika
from datetime import datetime, timedelta
import heapq
from collections import defaultdict, deque
from util.rmq import channel
from util.logger import logger_profile_service as logger

dotenv.load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')
AWS_DATA_BUCKET = os.getenv('AWS_DATA_BUCKET')

s3_client = boto3.client(
    's3',
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

##################################### PROFILE GENERATION #####################################

OS_TO_QUEUE = {
    '10': 'user-behavior-win-10',
    '11': 'user-behavior-win-11',
    'Mac': 'user-behavior-sequoia',
    'Ventura': 'user-behavior-ventura',
    'Monterey': 'user-behavior-monterey',
    'Big': 'user-behavior-big-sur',
    'Catalina': 'user-behavior-catalina',
    'Mojave': 'user-behavior-mojave',
}

FINGERPRINT_PATH = os.path.join(os.path.dirname(__file__), "fingerprints")
fingerprints = os.listdir(FINGERPRINT_PATH)

OS_TO_FINGERPRINTS = defaultdict(list)
for file_name in fingerprints:
    file_path = os.path.join(FINGERPRINT_PATH, file_name)
    with open(file_path, 'r') as file:
        data = json.load(file)
        platform_version = data['navigator']['userAgentData']['platformVersion']
        OS_TO_FINGERPRINTS[platform_version].append(data)

PROXY_LIST = []
with open(os.getenv('PROXY_LIST'), 'r') as file:
    for line in file:
        line = line.strip()
        if line:
            parts = line.split(':')
            if len(parts) == 5:
                id, host, port, username, password = parts
                PROXY_LIST.append({
                    'id': id,
                    'server': host + ':' + port,
                    'username': username,
                    'password': password
                })

profile_queue = []
for os_type, fingerprints in OS_TO_FINGERPRINTS.items():
    for fingerprint_idx in range(len(fingerprints)):
        for proxy_idx in range(len(PROXY_LIST)):
            profile = (os_type, fingerprint_idx, proxy_idx)
            profile_queue.append(profile)

random.shuffle(profile_queue)
profile_queue = deque(profile_queue)
category_profile_heaps = {}


def profile_to_information(profile):
    os_type, fingerprint_idx, proxy_idx = profile
    fingerprint = OS_TO_FINGERPRINTS[os_type][fingerprint_idx]
    proxy = PROXY_LIST[proxy_idx]
    queue = OS_TO_QUEUE[os_type]

    information = {
        'queue': queue,
        'proxy': proxy,
        'fingerprint': fingerprint,
        'os': os_type,
        'fingerprint_idx': fingerprint_idx,
        'proxy_idx': proxy_idx
    }
    return information

######################################## PROFILE SERVICE ########################################


def callback(ch, method, properties, body):
    s3_key = body.decode()
    logger.info(f" [x] Received {s3_key}")

    data = {}
    try:
        response = s3_client.get_object(Bucket=AWS_DATA_BUCKET, Key=s3_key)
        content = response['Body'].read()
        s3_data = json.loads(content)
        shop_id = s3_data['item']['shop_id']
        item_id = s3_data['item']['item_id']
        title = s3_data['item']['title']
        # sub-sub category
        category_id = s3_data['item']['categories'][-1]['catid']
        data = {
            'shop_id': shop_id,
            'item_id': item_id,
            'title': title,
            'category_id': category_id
        }
        query = s3_data['query']
    except Exception as e:
        logger.error(f"Error downloading or processing file: {e}")
        return

    # assign the profile to the task
    if data['category_id'] not in category_profile_heaps:
        cur_profile = profile_queue.popleft()
        profile_queue.append(cur_profile)
        category_profile_heaps[data['category_id']] = []
        heapq.heappush(
            category_profile_heaps[data['category_id']], (datetime.now(), cur_profile))
    else:
        last_timestamp, last_profile = heapq.heappop(
            category_profile_heaps[data['category_id']])
        if datetime.now() - last_timestamp > timedelta(seconds=60):
            # it's been 60 seconds since the last profile was used, so use again
            heapq.heappush(
                category_profile_heaps[data['category_id']], (datetime.now(), last_profile))
            cur_profile = last_profile
        else:
            heapq.heappush(
                category_profile_heaps[data['category_id']], (last_timestamp, last_profile))
            cur_profile = profile_queue.popleft()
            profile_queue.append(cur_profile)
            heapq.heappush(
                category_profile_heaps[data['category_id']], (datetime.now(), cur_profile))

    information = profile_to_information(cur_profile)
    information['item'] = data
    information['query'] = query

    try:
        # upload the information file to S3
        task_s3_key = s3_key.replace('.json', '_task.json')
        s3_client.put_object(
            Bucket=AWS_DATA_BUCKET,
            Key=task_s3_key,
            Body=json.dumps(information),
            ContentType="application/json"
        )
    except Exception as e:
        logger.error(f"Error uploading task file: {e}")
        return

    try:
        channel.queue_declare(queue=information['queue'], durable=True)

        # Publish the result to the next queue
        channel.basic_publish(
            exchange='', routing_key=information['queue'], body=task_s3_key, properties=pika.BasicProperties(
                expiration=os.getenv('RMQ_MESSAGE_EXPIRY_MS', '1800000'),
            ))

        logger.info(
            f" [x] Sent to '{information['queue']}': '{task_s3_key}'")
    except Exception as e:
        logger.error(f"Error publishing message: {e}")
        return


def main():

    # Declare the queue
    queue_name = os.getenv('PROFILE_QUEUE')
    channel.queue_declare(queue=queue_name, durable=True)

    # Subscribe to the queue
    channel.basic_consume(
        queue=queue_name, on_message_callback=callback, auto_ack=True)

    logger.info(
        f' [*] Waiting for messages from "{queue_name}". To exit press CTRL+C')
    channel.start_consuming()


if __name__ == "__main__":
    main()
