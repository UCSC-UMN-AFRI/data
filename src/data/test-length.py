from datetime import datetime
import json
from azure.cosmos import CosmosClient
from os import environ


def run():
    URL = 'https://ucsc-umn-afri-search-db.documents.azure.com:443/'
    KEY = ''
    COSMOS_DB_NAME = 'afri-search'
    client = CosmosClient(URL, credential=KEY)

    database = client.get_database_client(COSMOS_DB_NAME)
    CONTAINER_NAME = "leginfo_clean"
    container = database.get_container_client(CONTAINER_NAME)

    dict_items = []
    states = ['DC']
    from_year = 1980
    to_year = datetime.now().year
    to_year = 1989
    search_keys = ('food', 'veterinary')

    query = f"""
        SELECT VALUE COUNT(1)
        FROM c
        WHERE (c.year BETWEEN {from_year} AND {to_year})
    """

    if len(states) > 0:
        query += f""" AND c.state IN ({','.join([f"'{state}'" for state in states])})"""

    if len(search_keys) > 0:
        query += f""" AND ARRAY_CONTAINS_ALL(c.search_keys, {','.join([f"'{key}'" for key in search_keys])})"""


    print(query)

    items = container.query_items(
        query=query,
        enable_cross_partition_query=True
    )

    for item in items:
        dict_items.append(item)

    print(dict_items)


if __name__ == "__main__":
    run()

states = ["OH", "NY", "NV", "NJ", "NH", "MN", "MI", "ME", "KY", "KS", "GA", "DC", "CA", "AR"]
