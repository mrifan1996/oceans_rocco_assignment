import os
import unittest
from unittest.mock import MagicMock, patch

from app import handle_message


class TestImageResizing(unittest.TestCase):

    @patch("app.process_message", return_value=True)
    @patch("app.delete_message")
    def test_handle_message_success(self, mock_delete_message, mock_process_message):
        # Create a mock message
        message = {
            "Body": '{"id": "test_id", "image_url": "http://example.com/image.jpg"}',
            "ReceiptHandle": "test_receipt_handle",
            "MessageId": "test_message_id",
            "Attributes": {"ApproximateReceiveCount": "1"},
        }

        # Call handle_message function
        handle_message(message, "test_queue_url")

        # Check if process_message and delete_message functions are called
        mock_process_message.assert_called_once_with(message)
        mock_delete_message.assert_called_once_with(
            "test_queue_url", message["ReceiptHandle"]
        )

    @patch("app.process_message", side_effect=Exception("Mock exception"))
    @patch("app.delete_message")
    @patch("app.sqs_client.send_message")
    @patch("app.get_queue_url", return_value="test_dlq_url")
    def test_handle_message_error(
        self,
        mock_get_queue_url,
        mock_send_message,
        mock_delete_message,
        mock_process_message,
    ):
        # Create a mock message
        message = {
            "Body": '{"id": "test_id", "image_url": "http://example.com/image.jpg"}',
            "ReceiptHandle": "test_receipt_handle",
            "MessageId": "test_message_id",
            "Attributes": {
                "ApproximateReceiveCount": "15"
            },  # Exceeds MAX_RETRIES_FROM_QUEUE
        }

        # Call handle_message function
        handle_message(message, "test_queue_url")

        # Check if process_message, delete_message, and send_message functions are called
        mock_process_message.assert_called_once_with(message)
        mock_delete_message.assert_called_once_with(
            "test_queue_url", message["ReceiptHandle"]
        )
        mock_get_queue_url.assert_called_once_with("DEAD_LETTER_QUEUE_NAME")
        mock_send_message.assert_called_once_with(
            QueueUrl="test_dlq_url", MessageBody=message["Body"]
        )


if __name__ == "__main__":
    unittest.main()
