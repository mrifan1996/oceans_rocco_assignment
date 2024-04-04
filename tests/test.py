import os
import shutil
import unittest
from unittest.mock import MagicMock, patch

from PIL import Image

from app import download_image, handle_message, process_message, resize_image


class TestDownloadImage(unittest.TestCase):

    def setUp(self):
        # Create temporary directories for testing
        os.makedirs("test_originals")
        os.makedirs("test_resized")

    def tearDown(self):
        # Remove temporary directories after testing
        shutil.rmtree("test_originals")
        shutil.rmtree("test_resized")

    @patch("requests.get")
    def test_download_image_success(self, mock_get):
        # Set up mock response for a successful download
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"Fake image content"
        mock_get.return_value = mock_response

        # Call download_image function
        download_image("http://example.com/image.jpg", "test_image.jpg")

        # Check if the file is downloaded successfully
        self.assertTrue(os.path.exists("test_image.jpg"))

    @patch("requests.get")
    def test_download_image_failure(self, mock_get):
        # Set up mock response for a failed download
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.reason = "Not Found"
        mock_get.return_value = mock_response

        # Call download_image function
        download_image("http://example.com/image.jpg", "test_image.jpg")

        # Check if the file is not downloaded
        self.assertFalse(os.path.exists("test_image.jpg"))

    def test_resize_image(self):
        # Create a test image
        test_image = Image.new("RGB", (800, 600), color="red")
        test_image_path = "test_image.jpg"
        test_image.save(test_image_path)

        # Call resize_image function
        output_path = "resized_image.jpg"
        resize_image(test_image_path, output_path)

        # Check if the resized image exists
        self.assertTrue(os.path.exists(output_path))

        # Check the size of the resized image
        with Image.open(output_path) as resized_image:
            self.assertEqual(
                resized_image.size, (256, 192)
            )  # Height should be resized to 192

        # Clean up: remove the test image and resized image
        os.remove(test_image_path)
        os.remove(output_path)

    @patch("app.download_image")
    @patch("app.resize_image")
    @patch("app.logger.info")
    def test_process_message(
        self, mock_logger_info, mock_resize_image, mock_download_image
    ):
        # Create a dummy message
        message = {
            "Body": '{"id": "test_id", "image_url": "http://example.com/image.jpg"}',
            "MessageId": "test_message_id",
        }

        # Call process_message function
        processed = process_message(message)

        # Check if the function returns True
        self.assertTrue(processed)

        # Check if download_image and resize_image functions are called with correct arguments
        mock_download_image.assert_called_once_with(
            "http://example.com/image.jpg", "originals/test_id.jpg"
        )
        mock_resize_image.assert_called_once_with(
            "originals/test_id.jpg", "resized/test_id.jpg"
        )

        # Check if logger.info method is called with correct argument
        mock_logger_info.assert_called_once_with("Processed message test_message_id")

    @patch("app.process_message", return_value=True)
    @patch("app.delete_message")
    def test_handle_message_success(self, mock_delete_message, mock_process_message):
        # Create a dummy message
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
        # Create a dummy message
        message = {
            "Body": '{"id": "test_id", "image_url": "http://example.com/image.jpg"}',
            "ReceiptHandle": "test_receipt_handle",
            "MessageId": "test_message_id",
            "Attributes": {
                "ApproximateReceiveCount": "11"
            },  # Exceeds MAX_RETRIES_FROM_QUEUE
        }

        # Call handle_message function
        handle_message(message, "test_queue_url")

        # Check if process_message, delete_message, and send_message functions are called
        mock_process_message.assert_called_once_with(message)
        mock_delete_message.assert_called_once_with(
            "test_queue_url", message["ReceiptHandle"]
        )
        mock_send_message.assert_called_once_with(
            QueueUrl="test_dlq_url", MessageBody=message["Body"]
        )


if __name__ == "__main__":
    unittest.main()
