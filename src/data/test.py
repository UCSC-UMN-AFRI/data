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
    search_index = database.get_container_client("search_index")
    acts = database.get_container_client("acts")

    dict_items = []
    states = ['DC']
    from_year = 1980
    to_year = datetime.now().year
    to_year = 2025
    search_keys = ('food', 'veterinary')
    limit = 100
    offset = 0

    query = f"""
            SELECT c.act_num, c.relevance, c.search_key
            FROM c
            WHERE (c.year BETWEEN {from_year} AND {to_year})
            AND c.search_key IN ({','.join([f"'{key}'" for key in search_keys])})
    """

    if len(states) > 0:
        query += f""" AND c.state IN ({','.join([f"'{state}'" for state in states])})"""

    query += f"""
        ORDER BY c.relevance DESC
        OFFSET {offset} LIMIT {limit}
    """

    print(query)

    search_items = [item for item in search_index.query_items(
        query=query,
        enable_cross_partition_query=True
    )]
    print(search_items)

    search_items = [item for item in search_index.query_items(
        query=query,
        enable_cross_partition_query=True
    )]

    acts_to_fetch = [item['act_num'] for item in search_items]
    acts_items = acts.query_items(
        query=f"""SELECT * FROM c WHERE c.act_num IN ({','.join([f"'{act}'" for act in acts_to_fetch])})""",
        enable_cross_partition_query=True
    )

    act_data = { }
    for act_item in acts_items:
        act_data[act_item['act_num']] = act_item

    act_items = {}
    for search_item in search_items:
        act_item = act_data[search_item['act_num']]
        # truncate name to 500 characters
        if act_items.get(act_item['act_num']) is None:
            act_items[act_item['act_num']] = {
                "act_num": act_item['act_num'],
                "year": act_item['year'],
                "state": act_item['state'],
                "name": act_item['name'][:500] + '...' if len(act_item['name']) > 500 else act_item['name'],
                "link": act_item['link'],
                "backup_link": f"https://statelegislativedata.blob.core.windows.net/raw-data/{search_item['act_num']}.pdf",
                "relevances": []
            }

        act_items[act_item['act_num']]['relevances'].append({
            "score": search_item['relevance'],
            "search_key": search_item['search_key'],
        })

    dict_items = list(act_items.values())

    print(json.dumps(dict_items))

if __name__ == "__main__":
    run()
