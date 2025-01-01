from config import AZURE_BLOB_STORAGE_CONTAINER_NAME, AZURE_BLOB_STORAGE_ACCOUNT_NAME, AZURE_BLOB_STORAGE_CONNECTION_STRING
import duckdb

connection = duckdb.connect()

connection.execute("INSTALL httpfs")
connection.execute("LOAD httpfs")
connection.execute("INSTALL azure")
connection.execute("LOAD azure")
connection.execute(f"CREATE SECRET secret2 (TYPE AZURE, CONNECTION_STRING '{AZURE_BLOB_STORAGE_CONNECTION_STRING}');")

# Construct the Azure Blob Storage path
storage_account = AZURE_BLOB_STORAGE_ACCOUNT_NAME
container_name = AZURE_BLOB_STORAGE_CONTAINER_NAME
path = f"azure://{storage_account}.blob.core.windows.net/{container_name}/hashtag_data/*.parquet"

# sql = """
# SELECT 
#     date_trunc('minute', CAST(created_at AS TIMESTAMP)) AS minute,
#     hashtag,
#     COUNT(*) AS count
# FROM read_parquet('azure://stoltzmaniac/hashtag_data/*.parquet')
# GROUP BY minute, hashtag
# ORDER BY minute, hashtag
# """

sql = """
SELECT 
    hashtag,
    COUNT(*) AS count
FROM read_parquet('azure://stoltzmaniac/hashtag_data/*.parquet')
GROUP BY hashtag
ORDER BY count DESC
"""

query = connection.sql(sql)
results = query.to_df()
print(results)