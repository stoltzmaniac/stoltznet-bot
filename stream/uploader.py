import asyncio
import os
from azure.storage.blob import BlobServiceClient


def upload_bulk_to_azure(container_name: str, folder_path: str, connection_string: str):
    """
    Upload all files in the local folder (including subdirectories) to Azure Blob Storage
    and delete them after upload.
    """
    print(f"[DEBUG] Starting upload_bulk_to_azure for folder: {folder_path}")
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)

    # Recursively find all files in the folder
    for root, _, files in os.walk(folder_path):
        for file_name in files:
            file_path = os.path.join(root, file_name)
            # Construct relative blob path to preserve directory structure in Azure
            blob_name = os.path.relpath(file_path, folder_path).replace("\\", "/")

            try:
                blob_client = container_client.get_blob_client(f"hashtag_data/{blob_name}")
                print(f"[INFO] Uploading {blob_name} to Azure Blob Storage...")
                with open(file_path, "rb") as data:
                    blob_client.upload_blob(data, overwrite=True)
                print(f"[INFO] Successfully uploaded {blob_name}. Deleting local file...")
                os.remove(file_path)
            except Exception as e:
                print(f"[ERROR] Failed to upload {file_name}: {e}")


async def schedule_bulk_upload(container_name: str, folder_path: str, connection_string: str, interval: int):
    print("[DEBUG] schedule_bulk_upload started...")
    while True:
        print("[DEBUG] Running upload_bulk_to_azure...")
        try:
            await asyncio.get_event_loop().run_in_executor(
                None, upload_bulk_to_azure, container_name, folder_path, connection_string
            )
        except Exception as e:
            print(f"[ERROR] schedule_bulk_upload encountered an error: {e}")
        await asyncio.sleep(interval)
