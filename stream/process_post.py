import logging
import os
import time
import pandas as pd
import asyncio
from azure.storage.blob import BlobServiceClient


class PostProcessor:
    def __init__(self, folder_path: str, connection_string: str, container_name: str, max_buffer_size: int = 100 * 1024 * 1024, log_interval_seconds: int = 5):
        """
        Initialize the PostProcessor.

        Args:
            folder_path (str): Path to store temporary files locally.
            connection_string (str): Azure Blob Storage connection string.
            container_name (str): Azure Blob Storage container name.
            max_buffer_size (int): Maximum buffer size in bytes before writing to a file.
            log_interval_seconds (int): Time interval in seconds for logging the buffer size.
        """
        self.folder_path = folder_path
        self.connection_string = connection_string
        self.container_name = container_name
        self.max_buffer_size = max_buffer_size
        self.log_interval_seconds = log_interval_seconds
        self.buffer = pd.DataFrame()  # In-memory DataFrame buffer
        self.last_log_time = time.time()
        os.makedirs(folder_path, exist_ok=True)

    async def process_post_message(self, hashtags_of_interest: list, data: dict):
        """
        Process a single post, append it to the buffer, and handle file writing/uploading when the buffer is full.

        Args:
            hashtags_of_interest (list): List of hashtags to track.
            data (dict): Incoming data to process.
        """
        # Parse the data into rows
        parsed_rows = await self.parse_data(hashtags_of_interest, data)

        # Append rows to the buffer
        if parsed_rows:
            new_data = pd.DataFrame(parsed_rows)
            self.buffer = pd.concat([self.buffer, new_data], ignore_index=True)

            # Log buffer size at fixed intervals
            current_time = time.time()
            if current_time - self.last_log_time >= self.log_interval_seconds:
                buffer_size = self.buffer.memory_usage(deep=True).sum()
                logging.info(f"Current buffer size: {buffer_size / (1024 * 1024):.2f} MB")
                self.last_log_time = current_time

            # Check if the buffer size exceeds the limit
            buffer_size = self.buffer.memory_usage(deep=True).sum()
            if buffer_size >= self.max_buffer_size:
                await self.write_and_upload()

    async def parse_data(self, hashtags_of_interest: list, data: dict) -> list:
        """
        Parse the incoming data and extract relevant hashtags.

        Args:
            hashtags_of_interest (list): List of hashtags to track.
            data (dict): Incoming data to process.

        Returns:
            list: Parsed rows as dictionaries.
        """
        did = data.get("did", "")
        cid = data.get("commit", {}).get("cid", "")
        record = data.get("commit", {}).get("record", {})
        created_at = record.get("createdAt", "")
        text = record.get("text", "")
        facets = record.get("facets", [])

        parsed_rows = []
        for facet in facets:
            features = facet.get("features", [])
            for feature in features:
                if feature.get("$type") == "app.bsky.richtext.facet#tag":
                    raw_tag = feature.get("tag", "").strip().lower()
                    # if raw_tag in hashtags_of_interest:
                    parsed_rows.append({
                        "created_at": created_at,
                        "cid": cid,
                        "did": did,
                        "hashtag": raw_tag,
                        "text": text,
                    })

        return parsed_rows

    async def write_and_upload(self):
        """
        Write the buffer to a Parquet file, upload it to Azure, and reset the buffer.
        """
        if self.buffer.empty:
            logging.info("Buffer is empty, skipping write and upload.")
            return

        # Write the buffer to a Parquet file
        file_name = f"data_{int(time.time())}.parquet"
        file_path = os.path.join(self.folder_path, file_name)
        self.buffer.to_parquet(file_path, index=False, engine="pyarrow", compression="snappy")
        logging.info(f"Written buffer to file: {file_path}")

        # Reset the buffer
        self.buffer = pd.DataFrame()

        # Upload the file to Azure Blob Storage
        await self.upload_to_azure(file_path)

    async def upload_to_azure(self, file_path: str):
        """
        Upload the file to Azure Blob Storage and delete it locally.

        Args:
            file_path (str): Path to the file to upload.
        """
        blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
        blob_client = blob_service_client.get_blob_client(self.container_name, f"hashtag_data/{os.path.basename(file_path)}")

        try:
            logging.info(f"Uploading file to Azure: {file_path}")
            with open(file_path, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)
            logging.info(f"Successfully uploaded file: {file_path}")

            # Delete the local file after upload
            os.remove(file_path)
            logging.info(f"Deleted local file: {file_path}")
        except Exception as e:
            logging.error(f"Failed to upload file {file_path}: {e}")
