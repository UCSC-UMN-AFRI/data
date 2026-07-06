import asyncio
import gc
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from os import environ
from os.path import abspath, dirname, join
from threading import Lock
from urllib.parse import quote

import requests
from alive_progress import alive_it
from azure.cosmos.aio import CosmosClient
from dotenv import load_dotenv

# States to verify (match states_to_upload in upload_data.py; FL excluded by omission)
STATES_TO_VERIFY = [
    # "RI",
    # "CA",
    "IL",
    "RI",
    "ME",
    # "ME",
    # "RI",
    # "FL",
    # "KY",
    # "MT",
]

# Base URL for PDF checking
PDF_BASE_URL = "https://statelegislativedata.blob.core.windows.net/raw-data/"

# Global session for connection reuse
session = requests.Session()
session.headers.update(
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    }
)

# Thread-safe counters
results_lock = Lock()


async def fetch_uploaded_act_records(
    url: str, key: str, db_name: str, states: list[str]
):
    """
    Load act records from the Cosmos `acts` container (source of truth for uploads).
    Returns per state a list of {act_num, pdf_act_num} for PDF verification.
    """
    async with CosmosClient(
        url,
        credential=key,
        retry_total=9,
        retry_backoff_max=30,
    ) as client:
        database = client.get_database_client(db_name)
        acts = database.get_container_client("acts")
        results: dict[str, list[dict[str, str]]] = {}
        for state in states:
            records: list[dict[str, str]] = []
            query = "SELECT c.act_num, c.base_act_num FROM c WHERE c.state = @state"
            params = [{"name": "@state", "value": state}]
            async for item in acts.query_items(
                query=query,
                parameters=params,
            ):
                act_num = str(item["act_num"])
                base_act_num = item.get("base_act_num")
                pdf_act_num = str(base_act_num) if base_act_num else act_num
                records.append({"act_num": act_num, "pdf_act_num": pdf_act_num})
            results[state] = records
            unique_pdfs = len({r["pdf_act_num"] for r in records})
            print(
                f"  {state}: {len(records)} uploaded act_nums, "
                f"{unique_pdfs} unique PDFs to check"
            )
        return results


def check_pdf_url_exists(act_num, timeout=5, max_retries=2):
    """
    Check if a PDF exists at the specified URL format.
    Uses HEAD request to minimize resource usage.

    Args:
        act_num (str): The act number to check
        timeout (int): Request timeout in seconds
        max_retries (int): Maximum number of retry attempts

    Returns:
        tuple: (act_num: str, exists: bool, status_code: int, error: str)
    """
    if not act_num or act_num.strip() == "":
        return act_num, False, None, "Empty act_num"

    # Construct PDF URL
    pdf_filename = f"{act_num}.pdf"
    pdf_url = PDF_BASE_URL + quote(pdf_filename)

    for attempt in range(max_retries):
        try:
            # Use HEAD request to check existence without downloading
            response = session.head(pdf_url, timeout=timeout, allow_redirects=True)

            if response.status_code == 200:
                return act_num, True, response.status_code, None
            elif response.status_code == 404:
                return act_num, False, response.status_code, "Not found"
            else:
                return (
                    act_num,
                    False,
                    response.status_code,
                    f"HTTP {response.status_code}",
                )

        except requests.exceptions.Timeout:
            if attempt < max_retries - 1:
                time.sleep(0.1)  # Brief pause before retry
                continue
            return act_num, False, None, "Timeout"
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                time.sleep(0.1)  # Brief pause before retry
                continue
            return act_num, False, None, f"Request error: {str(e)}"

    return act_num, False, None, "Max retries exceeded"


def process_pdf_act_nums_batch(
    pdf_act_nums: list[str],
    pdf_to_uploaded: dict[str, list[str]],
    max_workers=20,
):
    """
    Process a batch of pdf_act_num values concurrently.

    Args:
        pdf_act_nums: Unique PDF blob names to check
        pdf_to_uploaded: Mapping pdf_act_num -> list of uploaded act_nums
        max_workers: Maximum number of concurrent threads

    Returns:
        dict: Results dictionary with counts and details
    """
    results = {
        "pdf_exists_count": 0,
        "pdf_missing_count": 0,
        "pdf_error_count": 0,
        "missing_act_nums": [],
        "error_act_nums": [],
        "details": [],
    }

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_pdf = {
            executor.submit(check_pdf_url_exists, pdf_act_num): pdf_act_num
            for pdf_act_num in pdf_act_nums
        }

        for future in alive_it(
            as_completed(future_to_pdf),
            total=len(pdf_act_nums),
            title="Checking URLs",
        ):
            try:
                pdf_act_num, exists, status_code, error = future.result()
                uploaded_act_nums = pdf_to_uploaded.get(pdf_act_num, [pdf_act_num])

                with results_lock:
                    if exists:
                        results["pdf_exists_count"] += 1
                    elif error and "Not found" not in error:
                        results["pdf_error_count"] += 1
                        results["error_act_nums"].append(
                            {
                                "pdf_act_num": pdf_act_num,
                                "uploaded_act_nums": uploaded_act_nums,
                                "status_code": status_code,
                                "error": error,
                            }
                        )
                    else:
                        results["pdf_missing_count"] += 1
                        results["missing_act_nums"].append(
                            {
                                "pdf_act_num": pdf_act_num,
                                "uploaded_act_nums": uploaded_act_nums,
                                "status_code": status_code,
                                "error": error or "Not found",
                            }
                        )

                    results["details"].append(
                        {
                            "pdf_act_num": pdf_act_num,
                            "uploaded_act_nums": uploaded_act_nums,
                            "exists": exists,
                            "status_code": status_code,
                            "error": error,
                        }
                    )

            except Exception as e:
                print(f"Error processing future: {e}")
                with results_lock:
                    results["pdf_error_count"] += 1
                    results["error_act_nums"].append(
                        {
                            "pdf_act_num": "unknown",
                            "uploaded_act_nums": [],
                            "status_code": None,
                            "error": f"Processing error: {str(e)}",
                        }
                    )

    return results


def process_act_nums_batch(act_nums, max_workers=20):
    """
    Process a batch of act_nums concurrently.

    Args:
        act_nums (list): List of act_num values to check
        max_workers (int): Maximum number of concurrent threads

    Returns:
        dict: Results dictionary with counts and details
    """
    results = {
        "pdf_exists_count": 0,
        "pdf_missing_count": 0,
        "pdf_error_count": 0,
        "missing_act_nums": [],
        "error_act_nums": [],
        "details": [],
    }

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_act_num = {
            executor.submit(check_pdf_url_exists, act_num): act_num
            for act_num in act_nums
        }

        # Process completed tasks with progress bar
        for future in alive_it(
            as_completed(future_to_act_num),
            total=len(act_nums),
            title="Checking URLs",
        ):
            try:
                act_num, exists, status_code, error = future.result()

                with results_lock:
                    if exists:
                        results["pdf_exists_count"] += 1
                    elif error and "Not found" not in error:
                        results["pdf_error_count"] += 1
                        results["error_act_nums"].append(
                            {
                                "act_num": act_num,
                                "status_code": status_code,
                                "error": error,
                            }
                        )
                    else:
                        results["pdf_missing_count"] += 1
                        results["missing_act_nums"].append(
                            {
                                "act_num": act_num,
                                "status_code": status_code,
                                "error": error or "Not found",
                            }
                        )

                    results["details"].append(
                        {
                            "act_num": act_num,
                            "exists": exists,
                            "status_code": status_code,
                            "error": error,
                        }
                    )

            except Exception as e:
                print(f"Error processing future: {e}")
                with results_lock:
                    results["pdf_error_count"] += 1
                    results["error_act_nums"].append(
                        {
                            "act_num": "unknown",
                            "status_code": None,
                            "error": f"Processing error: {str(e)}",
                        }
                    )

    return results


def write_missing_act_nums(
    state_code,
    missing_act_nums,
    error_act_nums,
    output_dir,
    source_label: str = "Cosmos acts container",
):
    """
    Write missing and error act_nums to files.

    Args:
        state_code (str): State code (e.g., "WV")
        missing_act_nums (list): List of missing act_nums with details
        error_act_nums (list): List of error act_nums with details
        output_dir (str): Output directory path
        source_label (str): Description for report headers (replaces per-CSV filename)
    """
    import os

    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Write missing act_nums (404s)
    if missing_act_nums:
        missing_file = join(output_dir, f"{state_code}_missing_act_nums.txt")
        with open(missing_file, "w") as f:
            f.write(f"Missing act_nums for {source_label} ({state_code})\n")
            f.write(f"Generated: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Total missing: {len(missing_act_nums)}\n")
            f.write("=" * 50 + "\n\n")

            for item in missing_act_nums:
                f.write(f"{item['pdf_act_num']}\n")
                uploaded = item.get("uploaded_act_nums") or []
                if len(uploaded) > 1:
                    f.write(f"  # uploaded act_nums: {', '.join(uploaded)}\n")

        print(f"  → Missing act_nums written to: {missing_file}")

    # Write error act_nums (timeouts, connection errors, etc.)
    if error_act_nums:
        error_file = join(output_dir, f"{state_code}_error_act_nums.json")
        with open(error_file, "w") as f:
            json.dump(
                {
                    "source": source_label,
                    "state_code": state_code,
                    "generated": time.strftime("%Y-%m-%d %H:%M:%S"),
                    "total_errors": len(error_act_nums),
                    "errors": error_act_nums,
                },
                f,
                indent=2,
            )

        print(f"  → Error act_nums written to: {error_file}")


def run():
    load_dotenv()

    url = environ["ACCOUNT_URI"]
    key = environ["ACCOUNT_KEY"]
    db_name = environ["COSMOS_DB_NAME"]

    script_dir = dirname(abspath(__file__))
    output_dir = join(script_dir, "missing_pdfs_output")

    print("Fetching uploaded act_nums from Cosmos DB (acts container)...")
    print(f"States: {', '.join(STATES_TO_VERIFY)}")
    print(f"Missing PDFs output directory: {output_dir}")
    print("-" * 50)

    act_records_by_state = asyncio.run(
        fetch_uploaded_act_records(url, key, db_name, STATES_TO_VERIFY)
    )

    print("Checking PDF URL availability with concurrent processing...")
    print("=" * 60)

    for i, state_code in enumerate(STATES_TO_VERIFY):
        records = act_records_by_state.get(state_code, [])
        print(f"\nProcessing [{state_code}] ({i + 1}/{len(STATES_TO_VERIFY)})")
        print(f"Uploaded act_nums from Cosmos: {len(records)}")

        if not records:
            print("No act_nums in Cosmos for this state, skipping PDF checks")
            print("-" * 50)
            continue

        pdf_to_uploaded: dict[str, list[str]] = {}
        for record in records:
            pdf_to_uploaded.setdefault(record["pdf_act_num"], []).append(record["act_num"])
        unique_pdf_act_nums = sorted(pdf_to_uploaded.keys())
        print(f"Unique PDFs to check: {len(unique_pdf_act_nums)}")

        start_time = time.time()
        results = process_pdf_act_nums_batch(
            unique_pdf_act_nums, pdf_to_uploaded, max_workers=30
        )
        end_time = time.time()

        total_uploaded = len(records)
        total_checked = len(unique_pdf_act_nums)

        if results["missing_act_nums"] or results["error_act_nums"]:
            write_missing_act_nums(
                state_code,
                results["missing_act_nums"],
                results["error_act_nums"],
                output_dir,
            )

        print(f"\nPDF URL Check Results for {state_code}:")
        print("=" * 50)
        print(f"Processing time: {end_time - start_time:.1f} seconds")
        if end_time > start_time:
            print(
                f"Average speed: {total_checked / (end_time - start_time):.1f} URLs/second"
            )

        exists_pct = (results["pdf_exists_count"] / total_checked) * 100
        missing_pct = (results["pdf_missing_count"] / total_checked) * 100
        error_pct = (results["pdf_error_count"] / total_checked) * 100

        print("\nPDF Availability:")
        print(f"  Uploaded act_nums: {total_uploaded}")
        print(f"  Unique PDFs checked: {total_checked}")
        print(
            f"  PDFs found:     {results['pdf_exists_count']:5}/{total_checked} ({exists_pct:.1f}%)"
        )
        print(
            f"  PDFs missing:   {results['pdf_missing_count']:5}/{total_checked} ({missing_pct:.1f}%)"
        )
        print(
            f"  Check errors:   {results['pdf_error_count']:5}/{total_checked} ({error_pct:.1f}%)"
        )

        print("-" * 50)
        del results
        gc.collect()

    gc.collect()


if __name__ == "__main__":
    try:
        run()
    finally:
        session.close()
    gc.collect()
