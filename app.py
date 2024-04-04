import json
import os
import time

import boto3
import requests
from PIL import Image
from structlog import get_logger

logger = get_logger()

# Constants
QUEUE_NAME = os.getenv("QUEUE_NAME")
REGION_NAME = os.getenv("REGION_NAME")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
MAX_RETRIES_FROM_QUEUE = 10
ORIGINALS_DIR = "originals"
RESIZED_DIR = "resized"

# Initializing the SQS client
sqs_client = boto3.client(
    "sqs",
    region_name=REGION_NAME,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)


def download_image(image_url, filename):
    response = requests.get(image_url)
    if response.status_code == 200:
        with open(filename, "wb") as f:
            f.write(response.content)
    else:
        logger.warning(f"Invalid URL: {response.url} {response.reason}")


# Defaulted size to 256 as this was the requirement but it can be overridden by passing a tuple value for size
def resize_image(image_path, output_path, size=(256, 256)):
    with Image.open(image_path) as img:
        # The thumbnail method of Pillows maintains the aspect ration and does not distort the picture as a result
        img.thumbnail(size)
        img.save(output_path)


def process_message(message):
    message_body = json.loads(message["Body"])
    image_id = message_body["id"]
    image_url = message_body["image_url"]

    # String manipulation to get the file extension
    file_extension = image_url.split(".")[-1]

    # Download original image
    original_filename = os.path.join(ORIGINALS_DIR, f"{image_id}.{file_extension}")
    download_image(image_url, original_filename)

    resized_filename = os.path.join(RESIZED_DIR, f"{image_id}.{file_extension}")
    resize_image(original_filename, resized_filename)

    logger.info(f"Processed message {message['MessageId']}")

    return True


def handle_message(message, queue_url):
    try:
        processed = process_message(message)
        if processed:
            # Delete the message from the queue if processed successfully
            delete_message(queue_url, message["ReceiptHandle"])
    except Exception as e:
        logger.error(f"Error processing message {message['MessageId']}: {e}")
        if (
            int(message["Attributes"]["ApproximateReceiveCount"])
            >= MAX_RETRIES_FROM_QUEUE
        ):
            # Message has been received more than 10 times, move to dead letter queue
            dead_letter_queue_url = get_queue_url(os.getenv("DEAD_LETTER_QUEUE_NAME"))
            sqs_client.send_message(
                QueueUrl=dead_letter_queue_url, MessageBody=message["Body"]
            )
            delete_message(queue_url, message["ReceiptHandle"])
            logger.warning(f"Message {message['MessageId']} moved to Dead Letter Queue and deleted from SQS queue")


def delete_message(queue_url, receipt_handle):
    sqs_client.delete_message(
        QueueUrl=queue_url, ReceiptHandle=receipt_handle
    )


def get_queue_url(queue_name):
    response = sqs_client.get_queue_url(QueueName=queue_name)
    return response["QueueUrl"]


if __name__ == "__main__":
    # Create the two directories if they have not been created already
    if not os.path.exists(ORIGINALS_DIR):
        os.makedirs(ORIGINALS_DIR)
    if not os.path.exists(RESIZED_DIR):
        os.makedirs(RESIZED_DIR)

    # Gets the queue URL using the queue name and the provided credentials
    queue_url = get_queue_url(QUEUE_NAME)

    while True:
        # Receive messages from SQS queue. We get only ApproximateReceiveCount attribute as we will use this to keep count of the number of retries for a message
        response = sqs_client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=10,  # Polls for 10 seconds before returning if no messages are found
            AttributeNames=["ApproximateReceiveCount"],
        )

        if "Messages" in response:
            for message in response["Messages"]:
                handle_message(message, queue_url)
        else:
            logger.info("No messages received, waiting...")
            # Set the sleep to 5 seconds as it will be easy to test when reviewing this app without having to wait long
            time.sleep(5)
