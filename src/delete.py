import asyncio
import re
from os import environ

from azure.cosmos.aio import CosmosClient
from dotenv import load_dotenv

# Parallel delete tuning: match upload_data.py; lower if heavily throttled (429).
CONCURRENCY = 64

# Set True to delete only act_num values ending in _1, _2, ... (upload suffix cleanup).
# Set False for full state delete (existing behavior).
SUFFIXED_ONLY = True

_SUFFIX_PATTERN = re.compile(r"_\d+$")


def _is_suffixed_act_num(act_num: str) -> bool:
    return bool(_SUFFIX_PATTERN.search(str(act_num)))


async def delete_suffixed_variants(
    url: str, key: str, db_name: str, state: str
) -> tuple[int, int]:
    async with CosmosClient(
        url,
        credential=key,
        retry_total=9,
        retry_backoff_max=30,
    ) as client:
        database = client.get_database_client(db_name)
        search = database.get_container_client("search_index")
        acts = database.get_container_client("acts")

        search_query = (
            "SELECT c.id, c.state, c.year, c.search_key, c.act_num "
            "FROM c WHERE c.state = @state AND CONTAINS(c.act_num, '_')"
        )
        search_params = [{"name": "@state", "value": state}]
        search_rows_raw = [
            r
            async for r in search.query_items(
                query=search_query,
                parameters=search_params,
            )
        ]
        search_rows = [
            r for r in search_rows_raw if _is_suffixed_act_num(r["act_num"])
        ]

        acts_query = (
            "SELECT c.id, c.act_num FROM c "
            "WHERE c.state = @state AND CONTAINS(c.act_num, '_')"
        )
        acts_params = [{"name": "@state", "value": state}]
        act_rows_raw = [
            r
            async for r in acts.query_items(
                query=acts_query,
                parameters=acts_params,
            )
        ]
        seen_act_num: set[str] = set()
        act_rows: list[dict] = []
        for r in act_rows_raw:
            ak = str(r["act_num"])
            if ak not in seen_act_num and _is_suffixed_act_num(ak):
                seen_act_num.add(ak)
                act_rows.append(r)

        sem = asyncio.Semaphore(CONCURRENCY)

        async def del_search(row: dict) -> None:
            async with sem:
                await search.delete_item(
                    item=row["id"],
                    partition_key=(
                        row["state"],
                        int(row["year"]),
                        row["search_key"],
                    ),
                )

        async def del_act(row: dict) -> None:
            async with sem:
                await acts.delete_item(
                    item=row["id"],
                    partition_key=row["act_num"],
                )

        await asyncio.gather(*(del_search(r) for r in search_rows))
        await asyncio.gather(*(del_act(r) for r in act_rows))

        return len(search_rows), len(act_rows)


async def delete_state(url: str, key: str, db_name: str, state: str) -> tuple[int, int]:
    async with CosmosClient(
        url,
        credential=key,
        retry_total=9,
        retry_backoff_max=30,
    ) as client:
        database = client.get_database_client(db_name)
        search = database.get_container_client("search_index")
        acts = database.get_container_client("acts")

        search_query = (
            "SELECT c.id, c.state, c.year, c.search_key FROM c WHERE c.state = @state"
        )
        search_params = [{"name": "@state", "value": state}]
        search_rows = [
            r
            async for r in search.query_items(
                query=search_query,
                parameters=search_params,
            )
        ]

        # Partition key for `acts` is /act_num (see upload_data + website search API), not /state.
        acts_query = "SELECT c.id, c.act_num FROM c WHERE c.state = @state"
        acts_params = [{"name": "@state", "value": state}]
        act_rows_raw = [
            r
            async for r in acts.query_items(
                query=acts_query,
                parameters=acts_params,
            )
        ]
        # One row per act; duplicates would cause concurrent double-delete -> 404.
        seen_act_num: set[str] = set()
        act_rows: list[dict] = []
        for r in act_rows_raw:
            ak = str(r["act_num"])
            if ak not in seen_act_num:
                seen_act_num.add(ak)
                act_rows.append(r)

        sem = asyncio.Semaphore(CONCURRENCY)

        async def del_search(row: dict) -> None:
            async with sem:
                await search.delete_item(
                    item=row["id"],
                    partition_key=(
                        row["state"],
                        int(row["year"]),
                        row["search_key"],
                    ),
                )

        async def del_act(row: dict) -> None:
            async with sem:
                await acts.delete_item(
                    item=row["id"],
                    partition_key=row["act_num"],
                )

        await asyncio.gather(*(del_search(r) for r in search_rows))
        await asyncio.gather(*(del_act(r) for r in act_rows))

        return len(search_rows), len(act_rows)


def run():
    load_dotenv()

    ## add states to delete here e.g.: ["GA", "IA"]
    states_to_delete = [
        "AL",
        "AK",
        "AZ",
        "AR",
        # "CA",
        "CO",
        "CT",
        "DE",
        "DC",
        "FL",
        "GA",
        "HI",
        "ID",
        "IL",
        "IN",
        "IA",
        "KS",
        "KY",
        "LA",
        "ME",
        "MD",
        "MA",
        "MI",
        "MN",
        "MS",
        "MO",
        "MT",
        "NE",
        "NV",
        "NH",
        "NJ",
        "NM",
        "NY",
        "NC",
        "ND",
        "OH",
        "OK",
        "OR",
        "PA",
        "RI",
        "SC",
        "SD",
        "TN",
        "TX",
        "UT",
        "VT",
        "VA",
        "WA",
        "WV",
        "WI",
        "WY",
    ]

    URL = environ["ACCOUNT_URI"]
    KEY = environ["ACCOUNT_KEY"]
    COSMOS_DB_NAME = environ["COSMOS_DB_NAME"]

    summary = []

    for state in states_to_delete:
        print("--------------------------------")
        if SUFFIXED_ONLY:
            print(f"About to delete suffixed variants (_N) for state: {state}")
        else:
            print(f"About to delete all data for state: {state}")
        confirm = input(
            f"Type the state code to confirm deletion of '{state}': "
        ).strip()
        if confirm != state:
            print(f"Confirmation did not match. Skipping {state}.")
            summary.append(
                {
                    "state": state,
                    "search_index_deleted": 0,
                    "acts_deleted": 0,
                    "skipped": True,
                }
            )
            continue

        if SUFFIXED_ONLY:
            print(f"Deleting suffixed variants for state {state}...")
            search_deleted, acts_deleted = asyncio.run(
                delete_suffixed_variants(URL, KEY, COSMOS_DB_NAME, state)
            )
        else:
            print(f"Deleting all data for state {state}...")
            search_deleted, acts_deleted = asyncio.run(
                delete_state(URL, KEY, COSMOS_DB_NAME, state)
            )

        summary.append(
            {
                "state": state,
                "search_index_deleted": search_deleted,
                "acts_deleted": acts_deleted,
                "skipped": False,
            }
        )
        mode = "suffixed variants" if SUFFIXED_ONLY else "all data"
        print(
            f"Done {state} ({mode}): search_index={search_deleted} items, "
            f"acts={acts_deleted} items"
        )

    print("================================")
    print("Summary:")
    for row in summary:
        if row["skipped"]:
            print(f"  {row['state']}: skipped (confirmation failed)")
        else:
            print(
                f"  {row['state']}: search_index deleted={row['search_index_deleted']}, "
                f"acts deleted={row['acts_deleted']}"
            )


if __name__ == "__main__":
    run()
