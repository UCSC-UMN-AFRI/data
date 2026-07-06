import gc
import json
from os import listdir
from os.path import abspath, dirname, isfile, join

import pandas as pd
from alive_progress import alive_it

from classification_utils import expand_classifications

dtype = {
    "state": str,
    "year": str,
    "act_num": str,
    "original_act_num": str,
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
    # Some act_nums in the classification file have spaces (e.g. "PA1975HB 189"),
    # but the clean data has them without spaces ("PA1975HB189"). Normalize here.
    df_classification["act_num"] = df_classification["act_num"].str.replace(
        " ", "", regex=False
    )
    df_classification.set_index(["act_num"], inplace=True)
    print("--------------------------------")

    # get all csv files in clean-data
    clean_data_dir = join(script_dir, "../data/clean-data")
    onlyfiles = sorted(
        [
            f
            for f in listdir(clean_data_dir)
            if isfile(join(clean_data_dir, f))
            and f.endswith(".csv")
             and f.split("_")[0] in ["CA"]
        ]
    )
    all_missing_classifications = {}
    all_multiple_classifications = {}

    for i, file in enumerate(onlyfiles):
        classification_missing = 0
        classification_missing_all = []
        classification_missing_examples = []
        non_ag_acts = 0
        search_keys_bad_format = 0
        search_keys_bad_format_examples = []
        multiple_classification = 0
        multiple_classification_all = []
        multiple_classification_examples = []
        nan_year_count = 0
        act_num_bad_format = 0
        act_num_missing = 0
        duplicate_act_num = 0
        print(f"Processing {file} ({i + 1}/{len(onlyfiles)})")
        df = load_csv(join(clean_data_dir, file))
        total_rows = df.shape[0]

        # Pre-compute duplicate act_nums for O(1) lookup instead of O(n²) scan
        act_num_counts = df["act_num"].value_counts()
        duplicate_act_nums = set(act_num_counts[act_num_counts > 1].index)

        df_grouped = df.groupby(["state", "year"])

        # Process each group
        for (state, year), group_df in alive_it(df_grouped, total=len(df_grouped)):
            # we skip these years for verification but if it's there it should be uploaded just not verified
            if int(year) >= 2022:
                continue
            # filter classification by year and state
            year_classification = df_classification[
                (df_classification["year"] == year)
                & (df_classification["state"] == state)
            ]

            # Process each row using itertuples() for better performance
            for row in group_df.itertuples(index=False):
                if not str(row.year).isnumeric():
                    nan_year_count += 1

                # should start with state and contain year (allows chapter numbers like DE2001CH123HB123)
                try:
                    if row.act_num is None or row.act_num == "":
                        act_num_missing += 1
                    else:
                        if not (
                            row.act_num.startswith(row.state)
                            and str(row.year) in row.act_num
                        ):
                            act_num_bad_format += 1
                        # check if act_num occurs more than once using pre-computed set
                        if row.act_num in duplicate_act_nums:
                            duplicate_act_num += 1
                except AttributeError:
                    act_num_missing += 1
                    continue

                try:
                    classification = year_classification.loc[row.act_num]
                except KeyError:
                    classification_missing += 1
                    classification_missing_all.append(row.act_num)
                    if len(classification_missing_examples) < 3:
                        classification_missing_examples.append(row.act_num)
                    continue

                classification_df = (
                    classification if isinstance(classification, pd.DataFrame) else None
                )
                expanded = expand_classifications(classification)

                if classification_df is not None and not (
                    classification_df == classification_df.iloc[0]
                ).all(axis=1).all():
                    if any(suffix is not None for suffix, _ in expanded):
                        multiple_classification += 1
                        search_keys_values = classification_df["search_keys"].tolist()
                        entry = {
                            "act_num": row.act_num,
                            "num_rows": len(classification_df),
                            "uni_bigrams_word_counts": search_keys_values,
                        }
                        multiple_classification_all.append(entry)
                        if len(multiple_classification_examples) < 3:
                            multiple_classification_examples.append(entry)

                if not expanded:
                    non_ag_acts += 1
                    continue

                for _suffix, classification_row in expanded:
                    search_keys = classification_row.get("search_keys")
                    if search_keys == "{}" or search_keys == "" or search_keys is None:
                        non_ag_acts += 1
                    else:
                        try:
                            json.loads(str(search_keys).replace("'", '"'))
                        except (json.JSONDecodeError, TypeError):
                            search_keys_bad_format += 1
                            if len(search_keys_bad_format_examples) < 3:
                                search_keys_bad_format_examples.append(
                                    {
                                        "act_num": row.act_num,
                                        "search_keys": search_keys,
                                    }
                                )

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
        # Non-ag acts are intentional filtering, not a data quality issue
        if non_ag_acts > 0:
            percentage = (non_ag_acts / total_rows) * 100
            print(
                f"Non-ag acts (excluded from search): {non_ag_acts:5}/{total_rows} ({percentage:.2f}%)"
            )
            print()

        class_stats = {
            "Act number classifications missing": classification_missing,
            "Act number expanded to suffixed variants (_1, _2, ...)": multiple_classification,
            "Search keys badly formatted (not a json)": search_keys_bad_format,
        }

        if any(class_stats.values()) > 0:
            for label, count in class_stats.items():
                if count > 0:
                    percentage = (count / total_rows) * 100
                    print(f"{label:30}: {count:5}/{total_rows} ({percentage:.2f}%)")
            # Show examples of classification issues
            if classification_missing_examples:
                print("\n  Examples of missing classifications:")
                for act_num in classification_missing_examples:
                    print(f"    - {act_num}")
            if multiple_classification_examples:
                print("\n  Examples of acts expanded to suffixed variants:")
                for ex in multiple_classification_examples:
                    print(f"    - act_num: {ex['act_num']}")
                    print(f"      num_rows: {ex['num_rows']}")
                    print(
                        f"      uni_bigrams_word_counts: {ex['uni_bigrams_word_counts']!r}"
                    )
            if search_keys_bad_format_examples:
                print("\n  Examples of badly formatted search_keys:")
                for ex in search_keys_bad_format_examples:
                    print(f"    - act_num: {ex['act_num']}")
                    print(f"      value: {ex['search_keys']!r}")
        else:
            print("No issues found")

        print("-" * 50 + "\n\n")

        if classification_missing_all:
            all_missing_classifications[file] = classification_missing_all
        if multiple_classification_all:
            all_multiple_classifications[file] = multiple_classification_all

        del df_grouped
        del df
        gc.collect()

    del df_classification

    if all_missing_classifications:
        output_path = join(script_dir, "../data/missing_classifications.txt")
        total_missing = sum(len(v) for v in all_missing_classifications.values())
        with open(output_path, "w") as f:
            f.write("Missing Classifications Report\n")
            f.write(f"Total missing: {total_missing}\n")
            f.write("=" * 50 + "\n\n")
            for file, act_nums in all_missing_classifications.items():
                f.write(f"{file} ({len(act_nums)} missing)\n")
                f.write("-" * 40 + "\n")
                for act_num in act_nums:
                    f.write(f"{act_num}\n")
                f.write("\n")
        print(f"Missing classifications written to: {output_path}")
        print(f"Total missing across all files: {total_missing}")
    else:
        print("No missing classifications found.")

    if all_multiple_classifications:
        output_path = join(script_dir, "../data/multiple_classifications.txt")
        total_multiple = sum(len(v) for v in all_multiple_classifications.values())
        with open(output_path, "w") as f:
            f.write("Multiple Classifications Report (expanded to suffixed variants)\n")
            f.write(f"Total expanded: {total_multiple}\n")
            f.write("=" * 50 + "\n\n")
            for file, entries in all_multiple_classifications.items():
                f.write(f"{file} ({len(entries)} expanded)\n")
                f.write("-" * 40 + "\n")
                for entry in entries:
                    f.write(f"act_num: {entry['act_num']}\n")
                    f.write(f"  rows: {entry['num_rows']}\n")
                    for idx, sk in enumerate(entry["uni_bigrams_word_counts"], 1):
                        f.write(f"  search_keys [{idx}]: {sk}\n")
                    f.write("\n")
        print(f"Multiple classifications written to: {output_path}")
        print(f"Total conflicts across all files: {total_multiple}")
    else:
        print("No multiple classifications found.")


def load_csv(file_path):
    print("Loading data from file...")
    # Get available columns first
    all_columns = pd.read_csv(file_path, nrows=0).columns

    # Define column mappings
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

    # Ensure year is always a string (original column name may not match dtype dict)
    if "year" in df.columns:
        df["year"] = df["year"].astype(str)

    # Strip spaces from act_num to match classification data
    if "act_num" in df.columns:
        df["act_num"] = df["act_num"].str.replace(" ", "", regex=False)

    # Replace all NaN values with None for proper JSON serialization
    df = df.where(pd.notna(df), None)

    return df


if __name__ == "__main__":
    run()
    gc.collect()
