import asyncio
import signal
import json
import websockets
from config import AZURE_BLOB_STORAGE_CONNECTION_STRING
from stream.process_post import process_post_message
from stream.uploader import schedule_bulk_upload


async def process_stream(hashtags_of_interest: list, folder_path: str):
    URI = "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post"
    message_count = 0
    while True:
        print("[INFO] Connecting to Jetstream...")
        try:
            async with websockets.connect(URI) as websocket:
                print("[INFO] Connected. Listening for posts...")
                while True:
                    try:
                        raw_msg = await asyncio.wait_for(websocket.recv(), timeout=5)
                        data = json.loads(raw_msg)
                        kind = data.get("kind")
                        if kind == "commit":
                            commit_data = data.get("commit", {})
                            collection = commit_data.get("collection")
                            if collection == "app.bsky.feed.post":
                                message_count += 1
                                if message_count % 100 == 0:
                                    print(f"[INFO] Processed message count: {message_count}")
                                await process_post_message(
                                    hashtags_of_interest=hashtags_of_interest,
                                    data=data,
                                    folder_path=folder_path,
                                )
                    except asyncio.TimeoutError:
                        continue  # If no message within 5 seconds, loop again
        except websockets.ConnectionClosed as e:
            print(f"[ERROR] WebSocket connection closed: {e}. Retrying in 5 seconds...")
            await asyncio.sleep(5)
        except Exception as e:
            print(f"[ERROR] Unexpected error: {e}. Retrying in 10 seconds...")
            await asyncio.sleep(10)


async def main():
    hashtags_to_track = [
        "rstats", "python", "stata", "sql", "html", "css", "javascript", "golang", "java", "csharp", "cplusplus", "trump", "loomer", "twitter", "musk", "elon"
    ]
    container_name = "stoltzmaniac"
    folder_path = "data"
    upload_interval = 10  # 20 seconds
    connection_string = AZURE_BLOB_STORAGE_CONNECTION_STRING

    if not connection_string:
        raise ValueError("[ERROR] AZURE_STORAGE_CONNECTION_STRING environment variable not set.")

    stream_task = asyncio.create_task(process_stream(hashtags_to_track, folder_path))
    upload_task = asyncio.create_task(schedule_bulk_upload(container_name, folder_path, connection_string, upload_interval))

    tasks = [stream_task, upload_task]

    def shutdown_handler():
        for task in tasks:
            task.cancel()

    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGINT, shutdown_handler)
    loop.add_signal_handler(signal.SIGTERM, shutdown_handler)

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        print("[INFO] Tasks have been cancelled. Shutting down gracefully.")


if __name__ == "__main__":
    # Run the main coroutine
    asyncio.run(main())
