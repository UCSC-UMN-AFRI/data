## Project overview

This project contains data preparation, verification, and upload utilities for legislative information. It helps you:
- Validate input CSVs and related classification data
- Check availability of source PDFs in Azure Blob Storage
- Upload cleaned records into Azure Cosmos DB
- Perform targeted cleanup against uploaded records

The code is organized under `src/` and expects input data under `data/`.

## Repository structure

- `src/verify_data.py`: Validates cleaned CSVs and their corresponding classification entries.
- `src/verify_uploaded_raw_pdfs.py`: Checks whether expected raw PDFs exist in Azure Blob Storage.
- `src/upload_data.py`: Uploads validated records to Azure Cosmos DB in batches.
- `src/delete.py`: Deletes items in Cosmos DB where `act_num` contains a newline character.
- `data/clean-data/`: Input CSVs per state (not committed).
- `data/classification_results.csv`: Classification results with search-key signals (not committed).
- `requirements.txt`: Python dependencies.
- `devbox.json`: Optional dev environment definition (Python 3.11 + uv/pip).

## Prerequisites

- Python 3.11
- Network access to Azure services (Blob for PDF checks; Cosmos DB for upload/delete)

Option A: Using a system Python/venv

```bash
python3.11 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
```

Option B: Using Devbox (optional)

```bash
devbox shell
uv pip install -r requirements.txt  # or: pip install -r requirements.txt
```

## Configuration

Create a `.env` file in the repository root with the following variables (needed for `upload_data.py` and `delete.py`):

```bash
ACCOUNT_URI="<cosmos-account-uri>"
ACCOUNT_KEY="<cosmos-account-key>"
COSMOS_DB_NAME="<cosmos-database-name>"
```

Other configuration points:
- `src/upload_data.py`: `states_to_upload` list controls which states are uploaded.
- `src/verify_uploaded_raw_pdfs.py`: `STATES_TO_PROCESS` set controls which states are checked.
- `src/verify_uploaded_raw_pdfs.py`: PDF base URL is `https://statelegislativedata.blob.core.windows.net/raw-data/`.
 - Cosmos DB container `leginfo_clean` uses hierarchical partitioning on `['/state','/year']`. Batch uploads provide `(state, int(year))` accordingly.

## Data inputs

Place input files in the `data/` directory:

- `data/clean-data/*.csv`: Cleaned rows by state; expected columns include:
  - `state`, `year`, `act_num`, `original_act_num`, `link`, `name`
  - The scripts also map common variants automatically: `State→state`, `Year→year`, `Title→name`, `links`/`Link to full text`→`link`.

- `data/classification_results.csv`: Must include columns: `act_num`, `year`, `state`, and `uni_bigrams_word_counts` (renamed to `search_keys` in code). The `search_keys` value should be a JSON-like mapping of token→count.

Important: `act_num` in the classification file is expected to match the cleaned data’s act identifier. Some flows derive this as `state + year + original_act_num`.

## How to run

Run commands from the repository root after installing dependencies and configuring `.env` as needed.

### 1) Validate CSVs and classifications

```bash
python src/verify_data.py
```

What it does:
- Loads each CSV under `data/clean-data/`
- Checks `act_num` formatting and duplicates
- Verifies `link` presence and `year` numeric
- Cross-checks with `classification_results.csv` for missing/empty/bad `search_keys`
- Prints per-file summaries and percentages

### 2) Verify raw PDF availability in blob storage

```bash
python src/verify_uploaded_raw_pdfs.py
```

What it does:
- Filters CSVs to process by `STATES_TO_PROCESS`
- Extracts unique `act_num` values and checks corresponding `PDF_BASE_URL/<act_num>.pdf`
- Uses concurrent requests for speed
- Writes outputs to `src/missing_pdfs_output/`:
  - `<STATE>_missing_act_nums.txt` (404s)
  - `<STATE>_error_act_nums.json` (timeouts, transient errors, etc.)

Adjust concurrency via `process_act_nums_batch(..., max_workers=30)` if needed.

### 3) Upload to Azure Cosmos DB

```bash
python src/upload_data.py
```

What it does:
- Loads `classification_results.csv` and builds `search_keys`
- Iterates `data/clean-data/*.csv`, filters by `states_to_upload`
- Standardizes `act_num` as `state + year + original_act_num` for both classification lookup and upload
- Batches records by `(state, year)` and uploads with batch size 100

Requirements/assumptions:
- The Cosmos container name is `leginfo_clean`.
- The container uses hierarchical partitioning `['/state','/year']`; batch calls use `(state, int(year))`.

### 4) Targeted cleanup in Cosmos DB

```bash
python src/delete.py
```

What it does:
- Connects to the same Cosmos DB container and deletes items where `act_num` contains a newline character (`"\n"`).

## Operational notes

- Batch size in uploads is 100 to respect Cosmos limits.
- `alive_progress` provides progress bars in long operations.
- `verify_data.py` and `verify_uploaded_raw_pdfs.py` only read local CSVs and external URLs; they do not modify data.
- `upload_data.py` converts `year` to integer before upload.

## Troubleshooting

- Cosmos DB batch errors about partition keys:
  - Ensure the container’s partition key definition matches the code’s usage (state/year as provided).
- Few or no records uploaded:
  - Check `states_to_upload` in `src/upload_data.py` (defaults to a narrow set).
  - Ensure `classification_results.csv` has matching `act_num` keys and non-empty `search_keys`.

## Notes

- `states_to_upload` is intentionally hardcoded in `src/upload_data.py`; adjust it directly in code as needed.


