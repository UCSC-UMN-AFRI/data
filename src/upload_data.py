import asyncio
import json
import sys
import time
import uuid
from azure.cosmos.aio import CosmosClient
from dotenv import load_dotenv
import pandas as pd
from os import listdir, environ
from os.path import isfile, join, dirname, abspath

# Parallel upload tuning: raise toward 256 if 429 retries stay rare; lower if throttled.
CONCURRENCY = 64
SEARCH_BATCH_SIZE = 100

dtype = {
    "state": str,
    "year": str,
    "act_num": str,
    "name": str,
}

NAME_FALLBACK_MAX_LEN = 500
TEXT_FALLBACK_COLS = ("Full text", "original_text")


def resolve_classification(
    classification: pd.DataFrame | pd.Series,
) -> pd.Series | None:
    """Match verify_data.py: collapse duplicate rows; skip truly conflicting rows."""
    if isinstance(classification, pd.Series):
        return classification

    first_row = classification.iloc[0]
    if (classification == first_row).all(axis=1).all():
        return first_row

    rows_are_equivalent = True
    for _, other_row in classification.iterrows():
        for col in classification.columns:
            first_val = first_row[col]
            other_val = other_row[col]
            if col == "search_keys":
                first_empty = first_val in ("{}", "", None)
                other_empty = other_val in ("{}", "", None)
                if first_empty != other_empty:
                    continue
            if first_val != other_val:
                rows_are_equivalent = False
                break
        if not rows_are_equivalent:
            break

    if not rows_are_equivalent:
        return None

    # Equivalent rows: prefer a row with non-empty search_keys for upload
    for _, r in classification.iterrows():
        sk = r.get("search_keys")
        if sk not in ("{}", "", None) and sk:
            try:
                parsed = json.loads(str(sk).replace("'", '"'))
                if parsed:
                    return r
            except (json.JSONDecodeError, TypeError):
                continue
    return first_row


def search_keys_to_dict(search_keys) -> dict | None:
    """Parse classification search_keys to a key->relevance dict, or None if skip row."""
    if search_keys in ("{}", "", None):
        return None
    try:
        keys_dict = json.loads(str(search_keys).replace("'", '"'))
    except (json.JSONDecodeError, TypeError):
        return None
    if not keys_dict:
        return None
    return keys_dict


async def upload_all(
    url: str,
    key: str,
    db_name: str,
    search_batches: dict,
    act_batches: dict,
) -> None:
    async with CosmosClient(
        url,
        credential=key,
        retry_total=9,
        retry_backoff_max=30,
    ) as client:
        database = client.get_database_client(db_name)
        search_container = database.get_container_client("search_index")
        act_container = database.get_container_client("acts")
        sem = asyncio.Semaphore(CONCURRENCY)

        async def send_search(state: str, year: str, search_key: str, batch: list) -> None:
            async with sem:
                await search_container.execute_item_batch(
                    batch,
                    partition_key=(state, int(year), search_key),
                )

        async def send_act(doc: dict) -> None:
            async with sem:
                await act_container.create_item(doc)

        coros = []
        for batch_key, value in search_batches.items():
            state, year, sk = batch_key.split("/", 2)
            for j in range(0, len(value), SEARCH_BATCH_SIZE):
                batch = value[j : j + SEARCH_BATCH_SIZE]
                coros.append(send_search(state, year, sk, batch))
        for act_doc in act_batches.values():
            coros.append(send_act(act_doc))

        await asyncio.gather(*coros)


def run():
    load_dotenv()

    URL = environ["ACCOUNT_URI"]
    KEY = environ["ACCOUNT_KEY"]
    COSMOS_DB_NAME = environ["COSMOS_DB_NAME"]

    ## add all states to upload here e.g.: ["GA", "IA"]
    states_to_upload = [
        # "AL",
        # "AK",
        # "AZ",
        # "AR",
        # "CA",
        # "CO",
        # "CT",
        # "DE",
        # "DC",
        # "FL",
        # "GA",
        # "HI",
        # "ID",
        # "IL",
        # "IN",
        # "IA",
        # "KS",
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
        # "OR",
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
        # "AR", "AZ", "CA", "CO", "CT",
    ]
    keys_to_upload = ["id", "act_num", "year", "state", "name"]

    script_dir = dirname(abspath(__file__))
    clean_data_dir = join(script_dir, "../data/clean-data")
    classification_path = join(script_dir, "../data/classification_results.csv")

    print("Loading classification results, this may take a while...")
    df_classification = pd.read_csv(
        classification_path,
        dtype={"year": str},
        usecols=["act_num", "year", "state", "uni_bigrams_word_counts"],
    )
    df_classification = df_classification[
        df_classification["state"].isin(states_to_upload)
    ]
    df_classification.rename(
        columns={"uni_bigrams_word_counts": "search_keys"}, inplace=True
    )
    df_classification["act_num"] = df_classification["act_num"].str.replace(
        " ", "", regex=False
    )
    df_classification.set_index(["act_num"], inplace=True)
    classification_by_act = {
        act: g for act, g in df_classification.groupby(level="act_num", sort=False)
    }
    print("--------------------------------")

    onlyfiles = sorted(
        f
        for f in listdir(clean_data_dir)
        if isfile(join(clean_data_dir, f)) and f.endswith(".csv")
    )

    total_upload_seconds = 0.0
    total_upload_rows = 0

    for i, file in enumerate(onlyfiles):
        if file.split("_")[0] not in states_to_upload:
            continue
        print(f"Processing {file} ({i + 1}/{len(onlyfiles)})")
        df = load_csv(join(clean_data_dir, file))
        total_rows = df.shape[0]

        search_batches = {}
        act_batches = {}
        df_columns = list(df.columns)
        for tup in df.itertuples(index=False, name=None):
            row = dict(zip(df_columns, tup))
            if row.get("name") in (None, ""):
                continue

            act_num = row.get("act_num")
            if act_num is None or pd.isna(act_num):
                continue
            act_num = str(act_num).strip()
            if not act_num:
                continue

            classification = classification_by_act.get(act_num)
            if classification is None:
                continue

            classification = resolve_classification(classification)
            if classification is None:
                continue

            search_keys_dict = search_keys_to_dict(classification["search_keys"])
            if not search_keys_dict:
                continue

            for search_key, relevance in search_keys_dict.items():
                search_data = {
                    "id": str(uuid.uuid4()),
                    "act_num": act_num,
                    "relevance": relevance,
                    "state": str(row["state"]),
                    "year": int(row["year"]),
                    "search_key": search_key,
                }
                batch_key = f"{row['state']}/{row['year']}/{search_key}"
                search_batches.setdefault(batch_key, []).append(
                    ("create", (search_data,), {})
                )

            data = dict(row)
            data["act_num"] = act_num
            data["year"] = int(data["year"])
            act_doc = {key: data.get(key) for key in keys_to_upload}
            act_batches.setdefault(act_num, act_doc)

        start_time = time.time()
        n_search_ops = sum(
            (len(v) + SEARCH_BATCH_SIZE - 1) // SEARCH_BATCH_SIZE
            for v in search_batches.values()
        )
        n_act_ops = len(act_batches)
        print(
            f"Uploading search index ({len(search_batches)} partitions, "
            f"{n_search_ops} batch ops) and acts ({n_act_ops} creates) "
            f"from {total_rows} rows to Cosmos DB (concurrency={CONCURRENCY})"
        )
        asyncio.run(upload_all(URL, KEY, COSMOS_DB_NAME, search_batches, act_batches))

        elapsed = time.time() - start_time
        total_upload_seconds += elapsed
        n_items = sum(len(v) for v in search_batches.values()) + len(act_batches)
        total_upload_rows += n_items
        if n_items > 0:
            print(
                f"Batch time for this file: {elapsed:.2f}s ({n_items} items uploaded)"
            )

    if total_upload_rows > 0:
        print(
            f"Average time per uploaded item: {total_upload_seconds / total_upload_rows:.4f} seconds"
        )
    else:
        print("No items were uploaded.")


def load_csv(file_path):
    print("Loading data from file...")
    all_columns = pd.read_csv(file_path, nrows=0).columns.tolist()

    column_mappings = {
        "State": "state",
        "Year": "year",
        "Title": "name",
        "title": "name",
        "summary": "name",
        "Summary": "name",
        "act_num": "act_num",
        "year": "year",
        "state": "state",
        "name": "name",
    }

    columns_to_load = [
        col
        for col in all_columns
        if col in column_mappings or col in column_mappings.values()
    ]
    for c in TEXT_FALLBACK_COLS:
        if c in all_columns and c not in columns_to_load:
            columns_to_load.append(c)

    load_dtype = {k: v for k, v in dtype.items() if k in columns_to_load}
    for c in TEXT_FALLBACK_COLS:
        if c in columns_to_load:
            load_dtype[c] = str

    df = pd.read_csv(file_path, dtype=load_dtype, usecols=columns_to_load)

    for old_col, new_col in column_mappings.items():
        if old_col in df.columns:
            df.rename(columns={old_col: new_col}, inplace=True)

    # Title, summary, etc. can all map to "name"; then df["name"] is a DataFrame, not a Series
    if "name" in df.columns:
        name_positions = [i for i, c in enumerate(df.columns) if c == "name"]
        if len(name_positions) > 1:
            combined = df.iloc[:, name_positions[0]]
            for pos in name_positions[1:]:
                combined = combined.combine_first(df.iloc[:, pos])
            df.drop(columns=["name"], inplace=True)
            df["name"] = combined

    if "year" in df.columns:
        df["year"] = df["year"].astype(str)

    if "act_num" in df.columns:
        df["act_num"] = df["act_num"].str.replace(" ", "", regex=False)

    has_name_col = "name" in df.columns
    text_col = None
    if "Full text" in df.columns:
        text_col = "Full text"
    elif "original_text" in df.columns:
        text_col = "original_text"

    if not has_name_col:
        if text_col is None:
            print(
                "ERROR: No name column after mappings, and neither "
                "'Full text' nor 'original_text' was found. Exiting."
            )
            sys.exit(1)
        df["name"] = df[text_col].astype(str).str.slice(0, NAME_FALLBACK_MAX_LEN)
        df.drop(columns=[c for c in TEXT_FALLBACK_COLS if c in df.columns], inplace=True)
    else:
        if text_col is not None:
            mask = df["name"].isna() | (df["name"].astype(str).str.strip() == "")
            if mask.any():
                df.loc[mask, "name"] = (
                    df.loc[mask, text_col].astype(str).str.slice(0, NAME_FALLBACK_MAX_LEN)
                )
        df.drop(columns=[c for c in TEXT_FALLBACK_COLS if c in df.columns], inplace=True)

    # Drop unnamed index column if it was loaded by mistake
    for junk in ("Unnamed: 0",):
        if junk in df.columns:
            df.drop(columns=[junk], inplace=True)

    if df["name"].isna().all() or (
        df["name"].astype(str).str.strip() == ""
    ).all():
        print("ERROR: name is empty for all rows. Exiting.")
        sys.exit(1)

    df = df.where(pd.notna(df), None)

    df["id"] = [str(uuid.uuid4()) for _ in range(len(df))]

    return df


if __name__ == "__main__":
    run()
