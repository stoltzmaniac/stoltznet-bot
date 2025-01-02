from config import AZURE_BLOB_STORAGE_CONTAINER_NAME, AZURE_BLOB_STORAGE_ACCOUNT_NAME, AZURE_BLOB_STORAGE_CONNECTION_STRING
import duckdb
import datetime
import matplotlib.pyplot as plt
import pandas as pd
import warnings

warnings.filterwarnings("ignore", category=UserWarning, module="matplotlib.font_manager")

HASHTAGS = ('rstats', 'python', 'stata', 'sql', 'html', 'css', 'javascript', 'golang', 'csharp', 'cplusplus')

# DuckDB connection
connection = duckdb.connect()

connection.execute("INSTALL httpfs")
connection.execute("LOAD httpfs")
connection.execute("INSTALL azure")
connection.execute("LOAD azure")
connection.execute(f"CREATE SECRET secret2 (TYPE AZURE, CONNECTION_STRING '{AZURE_BLOB_STORAGE_CONNECTION_STRING}');")

# Construct the Azure Blob Storage path
storage_account = AZURE_BLOB_STORAGE_ACCOUNT_NAME
container_name = AZURE_BLOB_STORAGE_CONTAINER_NAME
path = f"azure://{container_name}/hashtag_data/*.parquet"


# Get the current UTC time
time_12_hours_ago_utc = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=12)).strftime('%Y-%m-%d %H:%M:%S')

sql = f"""
WITH filtered_data AS (
    SELECT 
        cid,
        did,
        hashtag,
        COUNT(hashtag) OVER (PARTITION BY cid) AS hashtag_count,
        date_trunc('minute', CAST(created_at AS TIMESTAMP WITH TIME ZONE)) AS rounded_time
    FROM read_parquet('{path}')
    WHERE CAST(created_at AS TIMESTAMP WITH TIME ZONE) >= TIMESTAMP '{time_12_hours_ago_utc}' AT TIME ZONE 'UTC'
    AND hashtag IN {HASHTAGS}
),
author_aggregation AS (
    SELECT 
        hashtag,
        COUNT(DISTINCT did) AS unique_authors,
        COUNT(*) AS total_mentions
    FROM filtered_data
    WHERE hashtag_count <= 5  -- Exclude posts with excessive hashtags
    GROUP BY hashtag
),
top_hashtags AS (
    SELECT 
        hashtag
    FROM author_aggregation
    WHERE unique_authors >= 2
    ORDER BY total_mentions DESC
    LIMIT 5
)
SELECT 
    date_trunc('minute', rounded_time) AS interval_5_minutes,
    hashtag,
    COUNT(*) AS total_mentions
FROM filtered_data
WHERE hashtag IN (SELECT hashtag FROM top_hashtags)
GROUP BY interval_5_minutes, hashtag
ORDER BY interval_5_minutes ASC, total_mentions DESC;
"""

# Execute the SQL query and convert to a DataFrame
query = connection.sql(sql)
results = query.to_df()

# Ensure interval_5_minutes is a datetime
results["interval_5_minutes"] = pd.to_datetime(results["interval_5_minutes"])

# Pivot the data for easier plotting
pivot_results = results.pivot(index="interval_5_minutes", columns="hashtag", values="total_mentions").fillna(0)

# Convert the index (timestamps) to "minutes back" from the most recent time
latest_time = pivot_results.index.max()
pivot_results.index = (latest_time - pivot_results.index).total_seconds() // 60  # Convert seconds to minutes

# Sort by ascending minutes back
pivot_results = pivot_results.sort_index(ascending=True)

# Make the data cumulative over time
cumulative_results = pivot_results.cumsum()
print(cumulative_results.columns)
cumulative_results = cumulative_results[cumulative_results.index > 0]

# Plot using XKCD style
with plt.xkcd():
    plt.figure(figsize=(12, 6))
    for hashtag in cumulative_results.columns:
        plt.plot(cumulative_results.index, cumulative_results[hashtag], label=hashtag)

    plt.title("Cumulative Hashtag Mentions Over Time (Last 12 Hours)", fontsize=16)
    plt.xlabel("Minutes Back", fontsize=12)
    plt.ylabel("Cumulative Mentions", fontsize=12)
    plt.legend(title="Hashtag", loc="upper left")
    plt.grid(True, linestyle="--", linewidth=0.5, alpha=0.6)
    plt.tight_layout()
    plt.show()