from azure.cosmos import CosmosClient
from dotenv import load_dotenv
from os import environ
from alive_progress import alive_it
from collections import defaultdict

def run():
    load_dotenv()

    URL = environ["ACCOUNT_URI"]
    KEY = environ["ACCOUNT_KEY"]
    COSMOS_DB_NAME = environ["COSMOS_DB_NAME"]
    client = CosmosClient(URL, credential=KEY)

    database = client.get_database_client(COSMOS_DB_NAME)
    CONTAINER_NAME = "search_index"
    container = database.get_container_client(CONTAINER_NAME)

    # Query all items
    items = container.query_items(
        query="SELECT * FROM c",
        enable_cross_partition_query=True
    )

    # Group items by act_num, keyword, year, and state
    grouped_items = defaultdict(list)
    for item in items:
        key = (item["act_num"], item["search_key"], item["year"], item["state"])
        grouped_items[key].append(item)

    # Delete duplicates, keeping only the first occurrence
    total_deleted = 0
    for key, items in alive_it(grouped_items.items()):
        if len(items) > 1:
            # Keep the first item, delete the rest
            for item in items[1:]:
                container.delete_item(
                    item=item,
                    partition_key=(item["state"], item["year"], item["search_key"])
                )
                total_deleted += 1

    print(f"Total duplicates deleted: {total_deleted}")

if __name__ == "__main__":
    run()
