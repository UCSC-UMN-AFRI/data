import gc
import requests
import pandas as pd
from alive_progress import alive_it
from os import listdir
from os.path import isfile, join, dirname, abspath
from urllib.parse import quote
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import json

# Configuration: States to process (add/remove state codes as needed)
STATES_TO_PROCESS = {
    'GA',  # Georgia
    'IA',  # Iowa
    'KS',  # Kansas
    'MI',  # Michigan
    'MN',  # Minnesota
    'MO',  # Missouri
    'NM',  # New Mexico
    'NY',  # New York
    'NC',  # North Carolina
    'OR',  # Oregon
    'PA',  # Pennsylvania
    'WV',  # West Virginia
    'WY',  # Wyoming
}

dtype = {
    "state": str,
    "year": str,
    "act_num": str,
    "original_act_num": str,
    "link": str,
    "name": str,
}

# Base URL for PDF checking
PDF_BASE_URL = "https://statelegislativedata.blob.core.windows.net/raw-data/"

# Global session for connection reuse
session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
})

# Thread-safe counters
results_lock = Lock()

def extract_state_code_from_filename(filename):
    """
    Extract state code from CSV filename.

    Args:
        filename (str): CSV filename like "WV_leginfo_clean.csv"

    Returns:
        str: State code (e.g., "WV") or None if not found
    """
    try:
        # Split by underscore and take the first part as state code
        state_code = filename.split('_')[0].upper()
        # Validate it's likely a state code (2 characters, all letters)
        if len(state_code) == 2 and state_code.isalpha():
            return state_code
    except (IndexError, AttributeError):
        pass
    return None

def should_process_file(filename, allowed_states):
    """
    Check if a CSV file should be processed based on state filtering.

    Args:
        filename (str): CSV filename
        allowed_states (set): Set of allowed state codes

    Returns:
        bool: True if file should be processed
    """
    if not allowed_states:  # If no filter set, process all files
        return True

    state_code = extract_state_code_from_filename(filename)
    return state_code in allowed_states if state_code else False

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
                return act_num, False, response.status_code, f"HTTP {response.status_code}"

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
        'pdf_exists_count': 0,
        'pdf_missing_count': 0,
        'pdf_error_count': 0,
        'missing_act_nums': [],
        'error_act_nums': [],
        'details': []
    }

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_act_num = {
            executor.submit(check_pdf_url_exists, act_num): act_num
            for act_num in act_nums
        }

        # Process completed tasks with progress bar
        for future in alive_it(as_completed(future_to_act_num), total=len(act_nums), title="Checking URLs"):
            try:
                act_num, exists, status_code, error = future.result()

                with results_lock:
                    if exists:
                        results['pdf_exists_count'] += 1
                    elif error and "Not found" not in error:
                        results['pdf_error_count'] += 1
                        results['error_act_nums'].append({
                            'act_num': act_num,
                            'status_code': status_code,
                            'error': error
                        })
                    else:
                        results['pdf_missing_count'] += 1
                        results['missing_act_nums'].append({
                            'act_num': act_num,
                            'status_code': status_code,
                            'error': error or "Not found"
                        })

                    results['details'].append({
                        'act_num': act_num,
                        'exists': exists,
                        'status_code': status_code,
                        'error': error
                    })

            except Exception as e:
                print(f"Error processing future: {e}")
                with results_lock:
                    results['pdf_error_count'] += 1
                    results['error_act_nums'].append({
                        'act_num': 'unknown',
                        'status_code': None,
                        'error': f"Processing error: {str(e)}"
                    })

    return results

def write_missing_act_nums(state_code, filename, missing_act_nums, error_act_nums, output_dir):
    """
    Write missing and error act_nums to files.

    Args:
        state_code (str): State code (e.g., "WV")
        filename (str): Original CSV filename
        missing_act_nums (list): List of missing act_nums with details
        error_act_nums (list): List of error act_nums with details
        output_dir (str): Output directory path
    """
    import os

    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Write missing act_nums (404s)
    if missing_act_nums:
        missing_file = join(output_dir, f"{state_code}_missing_act_nums.txt")
        with open(missing_file, 'w') as f:
            f.write(f"Missing act_nums for {filename} ({state_code})\n")
            f.write(f"Generated: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Total missing: {len(missing_act_nums)}\n")
            f.write("=" * 50 + "\n\n")

            for item in missing_act_nums:
                f.write(f"{item['act_num']}\n")

        print(f"  → Missing act_nums written to: {missing_file}")

    # Write error act_nums (timeouts, connection errors, etc.)
    if error_act_nums:
        error_file = join(output_dir, f"{state_code}_error_act_nums.json")
        with open(error_file, 'w') as f:
            json.dump({
                'filename': filename,
                'state_code': state_code,
                'generated': time.strftime('%Y-%m-%d %H:%M:%S'),
                'total_errors': len(error_act_nums),
                'errors': error_act_nums
            }, f, indent=2)

        print(f"  → Error act_nums written to: {error_file}")

def run():
    # Get the directory where the script is located
    script_dir = dirname(abspath(__file__))
    output_dir = join(script_dir, "missing_pdfs_output")

    # Get all CSV files in clean-data
    clean_data_dir = join(script_dir, "../data/clean-data")
    all_files = [
        f
        for f in listdir(clean_data_dir)
        if isfile(join(clean_data_dir, f)) and f.endswith(".csv")
    ]

    # Filter files based on state configuration
    filtered_files = [
        f for f in all_files
        if should_process_file(f, STATES_TO_PROCESS)
    ]

    # Show filtering results
    print(f"Found {len(all_files)} total CSV files")
    print(f"Filtering for states: {', '.join(sorted(STATES_TO_PROCESS))}")
    print(f"Selected {len(filtered_files)} CSV files to process")
    print(f"Missing PDFs output directory: {output_dir}")

    if len(filtered_files) < len(all_files):
        skipped_files = [f for f in all_files if f not in filtered_files]
        print(f"Skipped files: {', '.join(skipped_files[:5])}" + (f" and {len(skipped_files)-5} more" if len(skipped_files) > 5 else ""))

    print("Checking PDF URL availability with concurrent processing...")
    print("=" * 60)

    for i, file in enumerate(filtered_files):
        state_code = extract_state_code_from_filename(file)
        print(f"\nProcessing {file} [{state_code}] ({i+1}/{len(filtered_files)})")
        df = load_csv(join(clean_data_dir, file))

        if df.empty:
            print("No data found or missing act_num column, skipping...")
            continue

        total_rows = df.shape[0]

        # Get unique act_nums to avoid duplicate checks
        unique_act_nums = df['act_num'].dropna().astype(str)
        unique_act_nums = unique_act_nums[unique_act_nums.str.strip() != ""]
        unique_act_nums = unique_act_nums[unique_act_nums != "nan"]
        unique_act_nums = unique_act_nums.unique().tolist()

        act_num_missing = total_rows - len(df['act_num'].dropna())
        total_checked = len(unique_act_nums)

        print(f"Total rows in CSV: {total_rows}")
        print(f"Act numbers missing/empty: {act_num_missing}")
        print(f"Unique act_nums to check: {total_checked}")

        if total_checked == 0:
            print("No valid act_nums found to check")
            continue

        # Process all act_nums concurrently
        start_time = time.time()
        results = process_act_nums_batch(unique_act_nums, max_workers=30)
        end_time = time.time()

        # Write missing/error act_nums to files
        if results['missing_act_nums'] or results['error_act_nums']:
            write_missing_act_nums(
                state_code,
                file,
                results['missing_act_nums'],
                results['error_act_nums'],
                output_dir
            )

        # Print results
        print(f"\nPDF URL Check Results for {file}:")
        print("=" * 50)
        print(f"Processing time: {end_time - start_time:.1f} seconds")
        print(f"Average speed: {total_checked / (end_time - start_time):.1f} URLs/second")

        if total_checked > 0:
            exists_pct = (results['pdf_exists_count'] / total_checked) * 100
            missing_pct = (results['pdf_missing_count'] / total_checked) * 100
            error_pct = (results['pdf_error_count'] / total_checked) * 100

            print(f"\nPDF Availability:")
            print(f"  PDFs found:     {results['pdf_exists_count']:5}/{total_checked} ({exists_pct:.1f}%)")
            print(f"  PDFs missing:   {results['pdf_missing_count']:5}/{total_checked} ({missing_pct:.1f}%)")
            print(f"  Check errors:   {results['pdf_error_count']:5}/{total_checked} ({error_pct:.1f}%)")

        print("-" * 50)
        del df
        del results
        gc.collect()

def load_csv(file_path):
    """Load CSV file with proper column mapping and error handling."""
    print(f"Loading data from {file_path}...")

    try:
        # Get available columns first
        all_columns = pd.read_csv(file_path, nrows=0).columns
    except Exception as e:
        print(f"Error reading CSV headers: {e}")
        return pd.DataFrame()

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

    # Filter to only columns we care about (prioritize act_num)
    columns_to_load = [
        col
        for col in all_columns
        if col in column_mappings.keys() or col in column_mappings.values()
    ]

    # Ensure we have at least act_num column
    if "act_num" not in columns_to_load and "act_num" not in [column_mappings.get(col) for col in columns_to_load]:
        print("WARNING: No act_num column found in CSV")
        return pd.DataFrame()

    # Load only the columns that exist
    try:
        df = pd.read_csv(file_path, dtype=dtype, usecols=columns_to_load)
    except Exception as e:
        print(f"Error loading CSV: {e}")
        return pd.DataFrame()

    # Apply column renaming
    for old_col, new_col in column_mappings.items():
        if old_col in df.columns:
            df.rename(columns={old_col: new_col}, inplace=True)

    return df

if __name__ == "__main__":
    try:
        run()
    finally:
        session.close()
    gc.collect()
