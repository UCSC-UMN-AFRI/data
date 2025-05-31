import gc
import json
from alive_progress import alive_it
import pandas as pd
from os import listdir
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
    # Get the directory where the script is located
    script_dir = dirname(abspath(__file__))

    # open data/classification_results.csv
    print("Loading classification results, this may take a while...")
    # Read only necessary columns to save memory
    df_classification = pd.read_csv(
        join(script_dir, "../data/classification_results.csv"),
        dtype={"year": str},
        usecols=["act_num", "year", "state", "uni_bigrams_word_counts"],
    )
    df_classification.rename(
        columns={"uni_bigrams_word_counts": "search_keys"}, inplace=True
    )
    df_classification.set_index(["act_num"], inplace=True)
    print("--------------------------------")

    # get all csv files in clean-data
    clean_data_dir = join(script_dir, "../data/clean-data")
    onlyfiles = [
        f
        for f in listdir(clean_data_dir)
        if isfile(join(clean_data_dir, f)) and f.endswith(".csv")
    ]
    for i, file in enumerate(onlyfiles):
        classification_missing = 0
        search_keys_missing = 0
        search_keys_bad_format = 0
        multiple_classification = 0
        nan_year_count = 0
        link_missing = 0
        act_num_bad_format = 0
        act_num_missing = 0
        duplicate_act_num = 0
        print(f"Processing {file} ({i+1}/{len(onlyfiles)})")
        df = load_csv(join(clean_data_dir, file))
        total_rows = df.shape[0]
        df_grouped = df.groupby(["state", "year"])

        # Group by state and year, and process each group
        for (state, year), group_df in alive_it(df_grouped, total=len(df_grouped)):
            # filter classification by year and state
            year_classification = df_classification[
                (df_classification["year"] == year)
                & (df_classification["state"] == state)
            ]

            # Process each row in the group
            for _, row in group_df.iterrows():
                # check links missing, check act_num well formatted
                if not str(row["year"]).isnumeric():
                    nan_year_count += 1

                try:
                    if row["link"] == None or row["link"] == "":
                        link_missing += 1
                except KeyError:
                    link_missing += 1

                # should start with state+year
                try:
                    if row["act_num"] == None or row["act_num"] == "":
                        act_num_missing += 1
                    else:
                        if not row["act_num"].startswith(row["state"] + str(row["year"])):
                            act_num_bad_format += 1
                        # check if act_num occurs more than once in the clean data df
                        if df[df["act_num"] == row["act_num"]].shape[0] > 1:
                            duplicate_act_num += 1
                except KeyError:
                    act_num_missing += 1
                    continue

                try:
                    classification = year_classification.loc[row["act_num"]]
                except KeyError:
                    classification_missing += 1
                    continue

                # Check for multiple classifications where the data is different
                if isinstance(classification, pd.DataFrame):
                    # Check if all rows are identical by comparing with the first row
                    first_row = classification.iloc[0]
                    if not (classification == first_row).all(axis=1).all():
                        multiple_classification += 1
                        continue
                    classification = first_row

                # Check for missing search keys
                if (
                    classification["search_keys"] == "{}"
                    or classification["search_keys"] == ""
                    or classification["search_keys"] == None
                ):
                    search_keys_missing += 1
                else:
                    try:
                        json.loads(classification["search_keys"].replace("'", '"'))
                    except KeyError:
                        search_keys_bad_format += 1

                del classification

            del year_classification
            del group_df

        # Print statistics with better formatting
        print("\nCSV Check Results:")
        print("=" * 50)
        print(f"Total rows processed: {total_rows}")

        # Check if name column is missing (fixed the logic)
        if "name" not in df.columns:
            print("WARNING: Name column is missing")

        # Format percentages to 2 decimal places
        stats = {
            "Act numbers missing": act_num_missing,
            "Act numbers badly formatted": act_num_bad_format,
            "Act numbers duplicate": duplicate_act_num,
            "Years with invalid value": nan_year_count,
            "Links missing": link_missing,
        }

        if any(stats.values()) > 0:
            for label, count in stats.items():
                if count > 0:
                    percentage = (count / total_rows) * 100
                    print(f"{label:30}: {count:5}/{total_rows} ({percentage:.2f}%)")
        else:
            print("No issues found")

        print("\nClassification Check Results:")
        print("=" * 50)
        class_stats = {
            "Act number classifications missing": classification_missing,
            "Act number multiple classifications with different data": multiple_classification,
            "Search keys are empty": search_keys_missing,
            "Search keys badly formatted (not a json)": search_keys_bad_format,
        }

        if any(class_stats.values()) > 0:
            for label, count in class_stats.items():
                if count > 0:
                    percentage = (count / total_rows) * 100
                    print(f"{label:30}: {count:5}/{total_rows} ({percentage:.2f}%)")
        else:
            print("No issues found")

        print("-" * 50 + "\n\n")
        del df_grouped
        del df
        gc.collect()

    del df_classification


def load_csv(file_path):
    print(f"Loading data from file...")
    # Get available columns first
    all_columns = pd.read_csv(file_path, nrows=0).columns

    # Define column mappings
    column_mappings = {
        "State": "state",
        "Year": "year",
        "Title": "name",
        "links": "link",
        "Link to full text": "link",
        "act_num": "act_num",
        "year": "year",
        "state": "state",
        "name": "name",
        "link": "link",
    }

    # Filter to only columns we care about
    columns_to_load = [
        col
        for col in all_columns
        if col in column_mappings.keys() or col in column_mappings.values()
    ]

    # Load only the columns that exist
    df = pd.read_csv(file_path, dtype=dtype, usecols=columns_to_load)

    # Apply column renaming
    for old_col, new_col in column_mappings.items():
        if old_col in df.columns:
            df.rename(columns={old_col: new_col}, inplace=True)

    # Replace all NaN values with None for proper JSON serialization
    df = df.where(pd.notna(df), None)

    return df


if __name__ == "__main__":
    run()
    gc.collect()
