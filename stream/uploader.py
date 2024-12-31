import asyncio
import os
from azure.storage.blob import BlobServiceClient


def upload_bulk_to_azure(container_name: str, folder_path: str, connection_string: str):
    """Upload all files in the local folder to Azure Blob Storage and delete them after upload."""
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)

    for file_name in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file_name)
        if os.path.isfile(file_path):
            blob_client = container_client.get_blob_client(f"hashtag_data/{file_name}")
            print(f"[INFO] Uploading {file_name} to Azure Blob Storage...")
            with open(file_path, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)
            print(f"[INFO] Successfully uploaded {file_name}. Deleting local file...")
            os.remove(file_path)


async def schedule_bulk_upload(container_name: str, folder_path: str, connection_string: str, interval: int):
    """Schedule bulk uploads to Azure Blob Storage at regular intervals."""
    while True:
        print(f"[INFO] Running bulk upload from folder: {folder_path}")
        await asyncio.get_event_loop().run_in_executor(
            None, upload_bulk_to_azure, container_name, folder_path, connection_string
        )
        await asyncio.sleep(interval)
