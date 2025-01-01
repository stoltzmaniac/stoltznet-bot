import logging
from logging.handlers import RotatingFileHandler
import asyncio
import json
import websockets
from config import AZURE_BLOB_STORAGE_CONNECTION_STRING
from stream.process_post import PostProcessor


# Configure logging
LOG_FILE = "/var/log/postprocessor.log"

logging.basicConfig(
    level=logging.INFO,  # Default log level
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        RotatingFileHandler(LOG_FILE, maxBytes=20485760, backupCount=5),  # Log to file
        logging.StreamHandler()  # Log to console
    ]
)


# Configuration
FOLDER_PATH = "data"
CONTAINER_NAME = "stoltzmaniac"
HASHTAGS_TO_TRACK = [
    "rstats", "python", "stata", "sql", "html", "css", "javascript", "golang",
    "java", "csharp", "cplusplus", "trump", "loomer", "twitter", "musk", "elon"
]
URI = "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post"


async def process_stream(post_processor: PostProcessor):
    """
    Connect to Jetstream WebSocket and process incoming data stream.

    Args:
        post_processor (PostProcessor): The post processor instance.
    """
    logging.info("Connecting to Jetstream WebSocket...")
    while True:
        try:
            async with websockets.connect(URI) as websocket:
                logging.info("Connected. Listening for posts...")
                while True:
                    try:
                        raw_msg = await asyncio.wait_for(websocket.recv(), timeout=5)
                        data = json.loads(raw_msg)
                        kind = data.get("kind")
                        if kind == "commit":
                            commit_data = data.get("commit", {})
                            collection = commit_data.get("collection")
                            if collection == "app.bsky.feed.post":
                                await post_processor.process_post_message(HASHTAGS_TO_TRACK, data)
                    except asyncio.TimeoutError:
                        continue
        except websockets.ConnectionClosed as e:
            logging.error(f"WebSocket connection closed: {e}. Retrying in 5 seconds...")
            await asyncio.sleep(5)
        except Exception as e:
            logging.error(f"Unexpected error: {e}. Retrying in 10 seconds...")
            await asyncio.sleep(10)


async def main():
    # Ensure the Azure connection string is set
    if not AZURE_BLOB_STORAGE_CONNECTION_STRING:
        raise ValueError("AZURE_BLOB_STORAGE_CONNECTION_STRING is not set.")

    # Initialize the post processor
    post_processor = PostProcessor(FOLDER_PATH, AZURE_BLOB_STORAGE_CONNECTION_STRING, CONTAINER_NAME, max_buffer_size = 20485760, log_interval_seconds = 30)

    # Start processing the stream
    await process_stream(post_processor)


if __name__ == "__main__":
    asyncio.run(main())
