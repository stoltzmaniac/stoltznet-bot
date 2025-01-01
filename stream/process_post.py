import os
import pandas as pd
from datetime import datetime


async def parse_data(hashtags_of_interest: list, data: dict) -> list:
    """
    Extract relevant hashtags from a single post commit.
    Each hashtag will create a separate entry for easy grouping.
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
                parsed_rows.append({
                    "created_at": created_at,
                    "cid": cid,
                    "did": did,
                    "hashtag": raw_tag,
                    "text": text,
                })

    return parsed_rows


async def write_parquet_data(data: list, folder_path: str):
    """
    Write parsed data to partitioned Parquet files based on hashtag.
    """
    # Convert data to a DataFrame
    df = pd.DataFrame(data)

    if not df.empty:
        # Ensure `created_at` is parsed as datetime
        if 'created_at' in df.columns:
            df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')

        # Partition by hashtag
        for hashtag, group in df.groupby('hashtag'):
            # Construct the partitioned file path
            now = group['created_at'].min().strftime("%Y%m%d%H%M%S") if not group['created_at'].isna().all() else datetime.utcnow().strftime("%Y%m%d%H%M%S")
            partitioned_folder_path = os.path.join(folder_path, f"hashtag={hashtag}")
            os.makedirs(partitioned_folder_path, exist_ok=True)

            file_path = os.path.join(partitioned_folder_path, f"hashtag_data_{now}.parquet")

            # Write Parquet file
            group.to_parquet(
                file_path,
                index=False,
                engine="pyarrow",
                compression="snappy"
            )
            print(f"[INFO] Written data to {file_path}")
    else:
        print("[INFO] No data to write.")



async def process_post_message(hashtags_of_interest: list, data: dict, folder_path: str):
    """
    Process a single post commit and save data in the specified folder as a Parquet file.
    """
    # Extract hashtags
    parsed_data = await parse_data(hashtags_of_interest, data)
    if not parsed_data:
        return

    print(f"[INFO] Writing data to Parquet for: {len(parsed_data)} rows.")
    # Write the parsed data to the specified folder
    await write_parquet_data(parsed_data, folder_path)
