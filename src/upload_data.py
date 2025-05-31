import json
import time
import uuid
from alive_progress import alive_it
from azure.cosmos import CosmosClient
from dotenv import load_dotenv
import pandas as pd
from os import listdir, environ
from os.path import isfile, join, dirname, abspath

dtype = {
    "state": str,
    "year": str,
    "act_num": str,
    "original_act_num": str,
    "link": str,
    "name": str,
}


def run():
    load_dotenv()

    URL = environ["ACCOUNT_URI"]
    KEY = environ["ACCOUNT_KEY"]
    COSMOS_DB_NAME = environ["COSMOS_DB_NAME"]
    client = CosmosClient(URL, credential=KEY)

    states_to_upload = [
        "MN",
    ]
    keys_to_upload = ["id", "act_num", "year", "state", "name", "link", "search_keys"]

    database = client.get_database_client(COSMOS_DB_NAME)
    CONTAINER_NAME = "leginfo_clean"
    container = database.get_container_client(CONTAINER_NAME)

    # Get the directory where the script is located
    script_dir = dirname(abspath(__file__))

    # open data/classification_results.csv
    print("Loading classification results, this may take a while...")
    # Read only necessary columns to save memory
    df_classification = pd.read_csv(
        "../data/classification_results.csv", dtype={"year": str}
    )
    df_classification.rename(
        columns={"uni_bigrams_word_counts": "search_keys"}, inplace=True
    )
    df_classification = df_classification[
        df_classification["state"].isin(states_to_upload)
    ]
    df_classification.set_index(["act_num"], inplace=True)
    print("--------------------------------")

    # get all csv files in clean-data
    onlyfiles = [
        f
        for f in listdir("../data/clean-data")
        if isfile(join("../data/clean-data", f)) and f.endswith(".csv")
    ]
    for i, file in enumerate(onlyfiles):
        if file.split("_")[0] not in states_to_upload:
            continue
        print(f"Processing {file} ({i+1}/{len(onlyfiles)})")
        df = load_csv("../data/clean-data/" + file)
        total_rows = df.shape[0]

        # # Create batches of operations for bulk upload
        item_batches = {}
        # filter df_classification by state and year
        for _, row in df.iterrows():
            batch_key = f"{row['state']}/{row['year']}"
            if batch_key not in item_batches:
                item_batches[batch_key] = []
            data = row.to_dict()
            row["act_num"] = row["state"] + row["year"] + row["original_act_num"]
            try:
                classification = df_classification.loc[row["act_num"]]
            except KeyError:
                continue
            # Handle multiple classifications
            if isinstance(classification, pd.DataFrame):
                # Take the first classification or combine them as needed
                classification = classification.iloc[0]  # Takes first row

            data["search_keys"] = list(
                json.loads(classification["search_keys"].replace("'", '"')).keys()
            )
            if data["search_keys"] == []:
                continue
            data = {key: data[key] for key in keys_to_upload}
            data["year"] = int(data["year"])
            item_batches[batch_key].append(("create", (data,), {}))

        # # Execute batch operations in chunks of 100 (Azure Cosmos DB limit)
        batch_size = 100
        start_time = time.time()
        print(f"Uploading {total_rows} rows to Cosmos DB ({len(item_batches)} batches)")
        for key, value in alive_it(item_batches.items()):
            num_items = len(value)
            for i in range(0, num_items, batch_size):
                batch = value[i : i + batch_size]
                container.execute_item_batch(
                    batch, partition_key=(key.split("/")[0], int(key.split("/")[1]))
                )

    end_time = time.time()
    print(
        f"Time taken to upsert a row on average: {(end_time - start_time) / df.shape[0]} seconds",
    )


def load_csv(file_path):
    print(f"Loading data from file...")
    df = pd.read_csv(file_path, dtype=dtype)

    # Drop the first column as it's usually a row number
    if "Unnamed: 0" in df.columns[0]:
        df.drop(columns=["Unnamed: 0"], inplace=True)

    if "State" in df.columns:
        df.rename(columns={"State": "state"}, inplace=True)

    if "Year" in df.columns:
        df.rename(columns={"Year": "year"}, inplace=True)

    if "bill_num" in df.columns:
        df.rename(columns={"bill_num": "original_act_num"}, inplace=True)

    if "Title" in df.columns:
        df.rename(columns={"Title": "name"}, inplace=True)

    if "links" in df.columns:
        df.rename(columns={"links": "link"}, inplace=True)

    if "Link to full text" in df.columns:
        df.rename(columns={"Link to full text": "link"}, inplace=True)

    # Replace all NaN values with None for proper JSON serialization
    df = df.where(pd.notna(df), None)

    # Add UUID column to DataFrame
    df["id"] = [str(uuid.uuid4()) for _ in range(len(df))]

    return df


if __name__ == "__main__":
    run()
