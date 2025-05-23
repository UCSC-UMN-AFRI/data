from azure.cosmos import CosmosClient
from dotenv import load_dotenv
from os import environ
from alive_progress import alive_it

def run():
    load_dotenv()

    URL = environ["ACCOUNT_URI"]
    KEY = environ["ACCOUNT_KEY"]
    COSMOS_DB_NAME = environ["COSMOS_DB_NAME"]
    client = CosmosClient(URL, credential=KEY)

    database = client.get_database_client(COSMOS_DB_NAME)
    CONTAINER_NAME = "search_index"
    container = database.get_container_client(CONTAINER_NAME)

    # delete all items where CONTAINS(c.act_num, "\n")
    items = container.query_items(
        query="SELECT * FROM c",
        enable_cross_partition_query=True
    )

    item_batches = {}
    # filter df_classification by state and year
    for i, row in enumerate(items):
        print(f"{i}")
        container.delete_item(item=row, partition_key=(row["state"], row["year"], row["search_key"]))
        # batch_key = f"{row['state']}/{row['year']}"
        # if batch_key not in item_batches:
        #     item_batches[batch_key] = []
        # item_batches[batch_key].append(("delete", (row,), {}))

    # batch_size = 100
    # for key, value in alive_it(item_batches.items()):
    #     num_items = len(value)
    #     for i in range(0, num_items, batch_size):
    #         batch = value[i : i + batch_size]
    #         print(batch)
    #         container.execute_item_batch(
    #             batch, partition_key=(key.split("/")[0], int(key.split("/")[1]))
    #         )


if __name__ == "__main__":
    run()
