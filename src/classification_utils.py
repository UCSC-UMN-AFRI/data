"""Shared classification expansion logic for upload_data.py and verify_data.py."""

import json

import pandas as pd


def _search_keys_nonempty(search_keys) -> bool:
    if search_keys in ("{}", "", None):
        return False
    try:
        parsed = json.loads(str(search_keys).replace("'", '"'))
        return bool(parsed)
    except (json.JSONDecodeError, TypeError):
        return False


def _rows_are_equivalent(classification: pd.DataFrame, first_row: pd.Series) -> bool:
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
                return False
    return True


def _preferred_equivalent_row(classification: pd.DataFrame) -> pd.Series:
    first_row = classification.iloc[0]
    for _, r in classification.iterrows():
        if _search_keys_nonempty(r.get("search_keys")):
            return r
    return first_row


def expand_classifications(
    classification: pd.DataFrame | pd.Series,
) -> list[tuple[str | None, pd.Series]]:
    """Return (suffix, row) pairs. suffix is None for single upload, '_1'/'_2'/... for conflicts."""
    if isinstance(classification, pd.Series):
        return [(None, classification)]

    first_row = classification.iloc[0]
    if (classification == first_row).all(axis=1).all():
        return [(None, first_row)]

    if _rows_are_equivalent(classification, first_row):
        return [(None, _preferred_equivalent_row(classification))]

    non_empty_rows = [
        r for _, r in classification.iterrows() if _search_keys_nonempty(r.get("search_keys"))
    ]
    if not non_empty_rows:
        return []

    return [(f"_{i}", row) for i, row in enumerate(non_empty_rows, start=1)]
