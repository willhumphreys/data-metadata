#!/usr/bin/env python3

import argparse
import json
import os
import re
import shutil
from typing import Dict, Any

import boto3
import numpy as np
import pandas as pd # Assuming load_price_data returns a pandas DataFrame

# Assuming these modules exist and function as in the original code
from job_placer import put_trade_job_on_queue
from models import BatchParameters
from price_range_calculator import calculate_max_range, load_price_data
from s3_downloader import download_from_s3, DEFAULT_OUTPUT_DIR

# --- Constants ---
# *** Initial fixed step size for STOP and LIMIT ***
INITIAL_STOP_LIMIT_STEP_SIZE = 1000 # Renamed from INITIAL_PARAM_STEP_SIZE
# Fixed duration and output steps (can be adjusted if needed)
DURATION_STEP = 7
OUTPUT_STEP = 4
# Maximum combinations allowed per single AWS Batch job array (per scenario string)
MAX_COMBINATIONS_PER_JOB = 950000
# *** Maximum number of scenario PAIRS (long+short) to submit ***
MAX_SCENARIO_PAIRS = 50

# Default holding/filling times
DEFAULT_HOLD_DAYS = 14
DEFAULT_FILL_HOURS = 8

# Time windows for price range analysis (hours)
TIME_WINDOW_OFFSET_HOURS = 8
TIME_WINDOW_STOP_LIMIT_HOURS = 336 # 14 days


# --- clean_directory, numpy_encoder, sanitize_job_name, create_batch_parameters remain the same ---
# (Include the previous versions of these functions here)
def clean_directory(directory):
    """
    Clean (empty) the specified directory by removing all files and subdirectories.
    Creates the directory if it doesn't exist.

    Args:
        directory (str): Path to the directory to clean
    """
    if os.path.exists(directory):
        # Be cautious with rmtree, ensure it's the correct directory
        if directory and directory != '/' and os.path.isdir(directory):
             # Check if directory is not obviously system-critical before removing
            safe_to_remove = not directory.startswith(('/bin', '/boot', '/dev', '/etc', '/lib', '/proc', '/run', '/sbin', '/sys', '/usr', '/var'))
            if safe_to_remove:
                print(f"Removing contents of: {directory}")
                for item in os.listdir(directory):
                    item_path = os.path.join(directory, item)
                    try:
                        if os.path.isfile(item_path) or os.path.islink(item_path):
                            os.unlink(item_path)
                        elif os.path.isdir(item_path):
                            shutil.rmtree(item_path)
                    except Exception as e:
                        print(f'Failed to delete {item_path}. Reason: {e}')
            else:
                 print(f"Skipping removal of potentially critical directory: {directory}")
        else:
            print(f"Invalid or potentially dangerous directory path provided for cleaning: {directory}")

    else:
        print(f"Creating directory: {directory}")
        os.makedirs(directory)


def numpy_encoder(obj):
    """
    Helper function to convert numpy types to Python native types for JSON serialization.
    """
    if isinstance(obj, (np.integer, np.int_, np.intc, np.intp, np.int8,
                       np.int16, np.int32, np.int64, np.uint8,
                       np.uint16, np.uint32, np.uint64)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float64, np.float16, np.float32, np.float64)):
        return float(obj)
    elif isinstance(obj, (np.complex128, np.complex64, np.complex128)):
        return {'real': obj.real, 'imag': obj.imag}
    elif isinstance(obj, (np.ndarray,)):
        return obj.tolist()
    elif isinstance(obj, (np.bool_)):
        return bool(obj)
    elif isinstance(obj, (np.void)):
        return None
    # Add handling for pandas Timestamps if they appear
    elif pd.api.types.is_datetime64_any_dtype(obj):
         # Convert Timestamp to ISO 8601 string format
        try:
            # Check if it's a single Timestamp or an array/series
            if hasattr(obj, 'isoformat'):
                 return obj.isoformat()
            else: # Attempt to convert if it looks like a date object without isoformat
                 return str(obj)
        except Exception:
             return str(obj) # Fallback to string representation
    else:
        # For types not explicitly handled, try converting to string or raise error
        # depending on desired behavior. Returning the object might cause issues later.
        try:
            # Attempt a standard JSON serialization for unknown types
            json.dumps(obj)
            return obj
        except TypeError:
            # If standard JSON fails, convert to string as a fallback
            print(f"Warning: Converting unhandled type {type(obj)} to string.")
            return str(obj)


def sanitize_job_name(name):
    """
    Sanitize job name by replacing invalid characters with valid ones.
    AWS Batch job names can only contain letters, numbers, hyphens (-) and underscores (_).
    Max length 60.
    """
    sanitized = re.sub(r'[^a-zA-Z0-9\-_]', '_', name)
    return sanitized[:60] # Ensure max length

def create_batch_parameters(group_tag: str, scenario: str, ticker: str, trade_type: str, s3_key_min: str) -> BatchParameters:
    """
    Creates the BatchParameters object for submitting a job.
    """
    symbol_file = f"{ticker}-1mF.csv" # Assuming this file format based on original code
    base_symbol = ticker
    full_scenario = f"{scenario}___{'short___' if trade_type == 'short' else ''}{symbol_file}"

    sanitized_ticker = sanitize_job_name(ticker)
    # Generate unique but descriptive names, incorporating scenario hash for uniqueness if needed
    scenario_hash = hex(hash(scenario) & 0xffffffff)[2:] # Short hash
    base_job_name = f"{sanitized_ticker}-{group_tag}-{trade_type}-{scenario_hash}"

    trades_job_name = f"Trades-{base_job_name}"
    aggregate_job_name = f"Aggregate-{base_job_name}"
    graphs_job_name = f"Graphs-{base_job_name}" # May need further differentiation
    trade_extract_job_name = f"TradeExtract-{base_job_name}"
    py_trade_lens_job_name = f"PyTradeLens-{base_job_name}"
    trade_summary_job_name = f"TradeSummary-{base_job_name}"

    # Ensure names don't exceed AWS limits (typically 128 chars for job names, depends on context)
    trades_job_name = trades_job_name[:128]
    aggregate_job_name = aggregate_job_name[:128]
    graphs_job_name = graphs_job_name[:128]
    trade_extract_job_name = trade_extract_job_name[:128]
    py_trade_lens_job_name = py_trade_lens_job_name[:128]
    trade_summary_job_name = trade_summary_job_name[:128]

    queue_name = os.environ.get("AWS_BATCH_JOB_QUEUE", "fargateSpotTrades")

    return BatchParameters(
        aggregate_job_name=aggregate_job_name, base_symbol=base_symbol, full_scenario=full_scenario,
        graphs_job_name=graphs_job_name, queue_name=queue_name, scenario=scenario, symbol_file=symbol_file,
        trade_type=trade_type, trades_job_name=trades_job_name, group_tag=group_tag, s3_key_min=s3_key_min,
        trade_extract_job_name=trade_extract_job_name, py_trade_lens_job_name=py_trade_lens_job_name,
        trade_summary_job_name=trade_summary_job_name
    )


# --- calculate_combinations and format_scenario_string remain the same ---
def calculate_combinations(s_min, s_max, s_step, l_min, l_max, l_step, o_min, o_max, o_step, d_min, d_max, d_step, out_min, out_max, out_step):
    """Calculates the number of combinations for given parameter ranges."""
    s_step = max(1, s_step)
    l_step = max(1, l_step)
    o_step = max(1, o_step)
    d_step = max(1, d_step)
    out_step = max(1, out_step)

    num_s = max(0, (s_max - s_min) // s_step) + 1 if s_max >= s_min else 0
    num_l = max(0, (l_max - l_min) // l_step) + 1 if l_max >= l_min else 0
    num_o = max(0, (o_max - o_min) // o_step) + 1 if o_max >= o_min else 0
    num_d = max(0, (d_max - d_min) // d_step) + 1 if d_max >= d_min else 0
    num_out = max(0, (out_max - out_min) // out_step) + 1 if out_max >= out_min else 0

    if num_s == 0 or num_l == 0 or num_o == 0 or num_d == 0 or num_out == 0:
      return 0

    total = 1
    try:
        # Use float multiplication for intermediate steps to potentially avoid overflow
        # on extremely large numbers, although Python handles large integers well.
        total = float(num_s) * float(num_l) * float(num_o) * float(num_d) * float(num_out)
        # Check if total exceeds a reasonable threshold or becomes inf
        if total == float('inf') or total > float(np.iinfo(np.int64).max) * 10: # Check against large float
           print("Warning: Combination calculation resulted in excessively large number.")
           return float('inf')
        # Convert back to int if within reasonable bounds
        total = int(total)

    except OverflowError:
        print("Warning: Combination calculation resulted in overflow.")
        return float('inf')
    return total


def format_scenario_string(s_min, s_max, s_step, l_min, l_max, l_step, o_min, o_max, o_step, d_min, d_max, d_step, out_min, out_max, out_step):
    """Formats the scenario string."""
    return (f"s_{s_min}..{s_max}..{max(1,s_step)}___"
            f"l_{l_min}..{l_max}..{max(1,l_step)}___"
            f"o_{o_min}..{o_max}..{max(1,o_step)}___" # Offset step now different
            f"d_{d_min}..{d_max}..{max(1,d_step)}___"
            f"out_{out_min}..{out_max}..{max(1,out_step)}")


def generate_and_submit_scenarios(
    time_to_place_range: int,
    time_to_hold_range: int,
    ticker: str,
    s3_key_min: str,
    group_tag: str,
    time_to_hold_days: int,
    time_to_fill_hours: int
) -> Dict[str, Any]:
    """
    Generates scenario strings based on calculated ranges and derived steps,
    splits them if they exceed MAX_COMBINATIONS_PER_JOB, increases stop/limit
    step size (and recalculates offset step) if the number of resulting
    scenarios exceeds MAX_SCENARIO_PAIRS, and submits them to AWS Batch.
    """
    print("\n[3/3] Generating and submitting trade scenarios...")

    # *** Initialize Stop/Limit step size for the first attempt ***
    current_stop_limit_step_size = INITIAL_STOP_LIMIT_STEP_SIZE
    final_scenario_strings = []

    # *** Outer loop to adjust step size based on number of scenarios ***
    while True:
        # *** Calculate Offset step size based on current Stop/Limit step size ***
        current_offset_step_size = max(1, current_stop_limit_step_size // 10)

        print(f"\nAttempting scenario generation with Stop/Limit Step: {current_stop_limit_step_size}, Offset Step: {current_offset_step_size}")

        # --- 1. Define Parameter Ranges using current step sizes ---
        offset_min = -time_to_place_range - 1
        offset_max = time_to_place_range + 1
        offset_step = current_offset_step_size # Use calculated offset step

        stop_min = -time_to_hold_range - 1
        stop_max = -1
        stop_step = current_stop_limit_step_size # Use current stop/limit step

        limit_min = 1
        limit_max = time_to_hold_range + 1
        limit_step = current_stop_limit_step_size # Use current stop/limit step

        # Fixed duration and output ranges
        duration_min = time_to_hold_days
        duration_max = time_to_hold_days
        duration_step = DURATION_STEP

        output_min = time_to_fill_hours
        output_max = time_to_fill_hours
        output_step = OUTPUT_STEP

        # --- 2. Iteratively Split Scenarios (Inner Loop) ---
        initial_scenario_params = {
            's_min': stop_min, 's_max': stop_max, 's_step': stop_step,
            'l_min': limit_min, 'l_max': limit_max, 'l_step': limit_step,
            'o_min': offset_min, 'o_max': offset_max, 'o_step': offset_step, # Use specific offset_step
            'd_min': duration_min, 'd_max': duration_max, 'd_step': duration_step,
            'out_min': output_min, 'out_max': output_max, 'out_step': output_step,
        }

        scenarios_to_process = [initial_scenario_params]
        final_scenario_strings = [] # Reset for this attempt

        while scenarios_to_process:
            current_params = scenarios_to_process.pop(0)
            # Ensure all steps are positive before calculation/formatting
            current_params['s_step'] = max(1, current_params['s_step'])
            current_params['l_step'] = max(1, current_params['l_step'])
            current_params['o_step'] = max(1, current_params['o_step'])
            current_params['d_step'] = max(1, current_params['d_step'])
            current_params['out_step'] = max(1, current_params['out_step'])

            combinations = calculate_combinations(**current_params)

            if combinations == 0:
                # print(f"  -> Skipping scenario chunk due to invalid range (combinations=0): {format_scenario_string(**current_params)}")
                continue
            if combinations == float('inf'):
                 print(f"  -> Skipping scenario chunk due to overflow/excessive size: {format_scenario_string(**current_params)}")
                 continue


            # print(f"Checking scenario chunk: stops({current_params['s_min']}..{current_params['s_max']}), "
            #       f"limits({current_params['l_min']}..{current_params['l_max']}), "
            #       f"offsets({current_params['o_min']}..{current_params['o_max']})... Combinations: {combinations}") # Reduced verbosity

            if combinations <= MAX_COMBINATIONS_PER_JOB:
                scenario_str = format_scenario_string(**current_params)
                final_scenario_strings.append({
                    "scenario": scenario_str,
                    "combinations": combinations,
                    "params": current_params
                    })
            else:
                # Scenario too large, split the range with the most steps
                # Calculate number of steps for s, l, o using their respective steps from current_params
                num_s = (current_params['s_max'] - current_params['s_min']) // current_params['s_step'] if current_params['s_step'] > 0 and current_params['s_max'] >= current_params['s_min'] else 0
                num_l = (current_params['l_max'] - current_params['l_min']) // current_params['l_step'] if current_params['l_step'] > 0 and current_params['l_max'] >= current_params['l_min'] else 0
                num_o = (current_params['o_max'] - current_params['o_min']) // current_params['o_step'] if current_params['o_step'] > 0 and current_params['o_max'] >= current_params['o_min'] else 0

                split_param = ""
                # Determine split parameter based on largest number of steps (>0)
                if num_o > 0 and num_o >= num_s and num_o >= num_l:
                    split_param = "o"
                elif num_s > 0 and num_s >= num_l:
                    split_param = "s"
                elif num_l > 0:
                   split_param = "l"
                else:
                    print(f"  -> WARNING: Cannot determine parameter to split for large scenario ({combinations} combinations). Params: {current_params}. Keeping as is. This WILL exceed limits.")
                    scenario_str = format_scenario_string(**current_params)
                    final_scenario_strings.append({
                        "scenario": scenario_str, "combinations": combinations, "params": current_params
                    })
                    continue

                p_min = current_params[f'{split_param}_min']
                p_max = current_params[f'{split_param}_max']
                p_step = current_params[f'{split_param}_step'] # Get the correct step for the param being split

                num_steps_total = max(0, (p_max - p_min) // p_step)
                mid_point_steps = num_steps_total // 2

                if mid_point_steps <= 0:
                    print(f"  -> WARNING: Cannot split {split_param} range ({p_min}..{p_max} step {p_step}) further. Keeping as is. This might exceed limits.")
                    scenario_str = format_scenario_string(**current_params)
                    final_scenario_strings.append({
                        "scenario": scenario_str, "combinations": combinations, "params": current_params
                    })
                    continue

                p_split_val = p_min + mid_point_steps * p_step

                params1 = current_params.copy()
                params1[f'{split_param}_max'] = p_split_val

                params2 = current_params.copy()
                params2[f'{split_param}_min'] = p_split_val + p_step

                if params1[f'{split_param}_max'] >= params1[f'{split_param}_min']:
                     scenarios_to_process.insert(0, params1)
                # else: # Optional logging for discarded splits
                #      print(f"  -> Discarding invalid split range 1 for {split_param}: min={params1[f'{split_param}_min']}, max={params1[f'{split_param}_max']}")

                if params2[f'{split_param}_max'] >= params2[f'{split_param}_min']:
                     scenarios_to_process.insert(0, params2)
                # else: # Optional logging for discarded splits
                #      print(f"  -> Discarding invalid split range 2 for {split_param}: min={params2[f'{split_param}_min']}, max={params2[f'{split_param}_max']}")

        # --- End of Inner Splitting Loop ---

        num_scenario_pairs = len(final_scenario_strings)
        print(f"Generated {num_scenario_pairs} scenario pairs with Stop/Limit step {current_stop_limit_step_size}, Offset step {current_offset_step_size}.")

        if num_scenario_pairs <= MAX_SCENARIO_PAIRS:
            print(f"Number of scenario pairs ({num_scenario_pairs}) is within the limit ({MAX_SCENARIO_PAIRS}). Proceeding to submission.")
            break # Exit the outer loop
        else:
            print(f"Number of scenario pairs ({num_scenario_pairs}) exceeds the limit ({MAX_SCENARIO_PAIRS}).")
            # *** Double the Stop/Limit step size for the next attempt ***
            current_stop_limit_step_size *= 2
            print(f"Doubling Stop/Limit parameter step size to: {current_stop_limit_step_size}")
            # Offset step will be recalculated at the start of the next iteration

        # Safety break
        # Define stop_min/max etc. here again just for the safety check calculation range
        offset_min_safe = -time_to_place_range - 1; offset_max_safe = time_to_place_range + 1
        stop_min_safe = -time_to_hold_range - 1; stop_max_safe = -1
        limit_min_safe = 1; limit_max_safe = time_to_hold_range + 1
        # Check if stop/limit step exceeds the combined range significantly
        if current_stop_limit_step_size > abs(stop_min_safe - stop_max_safe) + abs(limit_min_safe-limit_max_safe): # Simplified check
             print("ERROR: Stop/Limit step size has become excessively large compared to ranges. Stopping generation.")
             final_scenario_strings = []
             return {'statusCode': 500, 'body': json.dumps({'message': 'Failed to generate scenarios within limits, step size grew too large.'})}

    # --- End of Outer Step Size Adjustment Loop ---

    # --- 3. Submit Jobs to AWS Batch ---
    if not final_scenario_strings:
        print("\nNo valid scenarios were generated after applying constraints. No jobs submitted.")
        return {'statusCode': 200, 'body': json.dumps({'message': 'No valid scenarios generated, no jobs submitted.'})}

    print(f"\nProceeding to submit {len(final_scenario_strings)} scenario pairs...")
    submitted_job_count = 0
    total_combinations_submitted = 0

    try:
        batch_client = boto3.client('batch')
    except Exception as e:
        print(f"Error creating boto3 batch client: {e}")
        return {'statusCode': 500, 'body': json.dumps({'message': 'Failed to create AWS Batch client.'})}

    for scenario_info in final_scenario_strings:
        scenario_str = scenario_info['scenario']
        combinations = scenario_info['combinations']

        # Submit LONG
        try:
            batch_params_long = create_batch_parameters(group_tag, scenario_str, ticker, "long", s3_key_min)
            put_trade_job_on_queue(batch_params_long, batch_client)
            submitted_job_count += 1
            total_combinations_submitted += combinations
            print(f"  Submitted LONG job: {batch_params_long.trades_job_name}")
        except Exception as e:
            print(f"  ERROR Submitting LONG job for scenario {scenario_str}: {e}")

        # Submit SHORT
        try:
            batch_params_short = create_batch_parameters(group_tag, scenario_str, ticker, "short", s3_key_min)
            put_trade_job_on_queue(batch_params_short, batch_client)
            submitted_job_count += 1
            total_combinations_submitted += combinations
            print(f"  Submitted SHORT job: {batch_params_short.trades_job_name}")
        except Exception as e:
            print(f"  ERROR Submitting SHORT job for scenario {scenario_str}: {e}")

    # --- 4. Save Configuration and Return Summary ---
    # Calculate the final offset step size used for reporting
    final_offset_step_size = max(1, current_stop_limit_step_size // 10)
    output_summary = {
        'ticker': ticker,
        'group_tag': group_tag,
        's3_key_min': s3_key_min,
        'time_to_hold_days': time_to_hold_days,
        'time_to_fill_hours': time_to_fill_hours,
        'initial_stop_limit_step_size': INITIAL_STOP_LIMIT_STEP_SIZE,
        'final_stop_limit_step_size': current_stop_limit_step_size,
        'final_offset_step_size': final_offset_step_size, # Report final offset step
        'max_combinations_per_job': MAX_COMBINATIONS_PER_JOB,
        'max_scenario_pairs_limit': MAX_SCENARIO_PAIRS,
        'price_analysis': {
            'offset_window_hours': TIME_WINDOW_OFFSET_HOURS,
            'offset_max_range': time_to_place_range,
            'stop_limit_window_hours': TIME_WINDOW_STOP_LIMIT_HOURS,
            'stop_limit_max_range': time_to_hold_range,
        },
        'submitted_job_pairs': submitted_job_count // 2,
        'total_combinations_submitted': total_combinations_submitted,
        'scenarios': final_scenario_strings
    }

    summary_filename = f"trader_config_summary_{sanitize_job_name(ticker)}_{group_tag}.json"
    try:
        output_file_path = os.path.join(".", summary_filename)
        with open(output_file_path, 'w') as f:
            json.dump(output_summary, f, indent=2, default=numpy_encoder)
        print(f"\nTrader configuration summary saved to {os.path.abspath(output_file_path)}")
    except Exception as e:
        print(f"\nError saving trader configuration summary: {e}")

    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': f'Successfully generated and submitted {submitted_job_count // 2} scenario pairs ({submitted_job_count} jobs) for {ticker} using final Stop/Limit step {current_stop_limit_step_size} and Offset step {final_offset_step_size}',
            'total_combinations_submitted': total_combinations_submitted,
            'summary_file': summary_filename
        }, default=numpy_encoder)
    }


# --- pipeline and main functions remain largely the same ---
def pipeline(ticker=None, output_dir=DEFAULT_OUTPUT_DIR, clean_output=True, group_tag=None,
             s3_key_min=None, time_to_hold_days=DEFAULT_HOLD_DAYS, time_to_fill_hours=DEFAULT_FILL_HOURS):
    """
    Main pipeline: Download data, calculate ranges, generate and submit scenarios.
    """
    if ticker is None: print("Error: Ticker is required"); raise ValueError("Ticker is required")
    if s3_key_min is None: print("Error: S3 key for minute data (s3_key_min) is required"); raise ValueError("S3 key for minute data is required")
    if group_tag is None: print("Error: Group tag is required for job identification"); raise ValueError("Group tag is required")

    print(f"Starting pipeline for Ticker: {ticker}, Group: {group_tag}")
    print(f"Hold Time: {time_to_hold_days} days, Fill Time: {time_to_fill_hours} hours")
    print(f"Initial Stop/Limit Step Size: {INITIAL_STOP_LIMIT_STEP_SIZE}, Max Combinations/Job: {MAX_COMBINATIONS_PER_JOB}, Max Scenario Pairs: {MAX_SCENARIO_PAIRS}")

    if clean_output: print(f"Cleaning output directory: {output_dir}"); clean_directory(output_dir)

    # --- 1. Download Data ---
    print(f"\n[1/3] Downloading data from S3 key: {s3_key_min}...")
    bucket = os.environ.get('MOCHI_DATA_BUCKET')
    if not bucket: print("Error: Environment variable MOCHI_DATA_BUCKET is not set."); raise ValueError("MOCHI_DATA_BUCKET environment variable not set.")

    file_path = download_from_s3(s3_key_min, output_dir, bucket=bucket)
    if not file_path or not os.path.exists(file_path): print(f"Error: Failed to download or locate data file from {s3_key_min}"); raise ValueError(f"Failed to download data from {s3_key_min}")
    print(f"Data downloaded to: {file_path}")

    # --- 2. Calculate Price Ranges ---
    print(f"\n[2/3] Calculating maximum price ranges...")
    try: df = load_price_data(file_path)
    except Exception as e: print(f"Error loading price data from {file_path}: {e}"); raise ValueError(f"Could not load price data for {ticker}") from e
    if df.empty: print(f"Error: No valid price data loaded for {ticker} from {file_path}"); raise ValueError("No valid price data loaded")

    print(f"Analyzing {TIME_WINDOW_OFFSET_HOURS} hour window for offset range...")
    place_results = calculate_max_range(df, TIME_WINDOW_OFFSET_HOURS)
    time_to_place_range = int(numpy_encoder(place_results['max_range']))
    print(f"  -> Max range for offset ({TIME_WINDOW_OFFSET_HOURS} hrs): {time_to_place_range}")

    print(f"Analyzing {TIME_WINDOW_STOP_LIMIT_HOURS} hour window for stop/limit range...")
    hold_results = calculate_max_range(df, TIME_WINDOW_STOP_LIMIT_HOURS)
    time_to_hold_range = int(numpy_encoder(hold_results['max_range']))
    print(f"  -> Max range for stop/limit ({TIME_WINDOW_STOP_LIMIT_HOURS} hrs): {time_to_hold_range}")

    # --- 3. Generate and Submit Scenarios ---
    submission_results = generate_and_submit_scenarios(
        time_to_place_range=time_to_place_range, time_to_hold_range=time_to_hold_range, ticker=ticker,
        s3_key_min=s3_key_min, group_tag=group_tag, time_to_hold_days=time_to_hold_days, time_to_fill_hours=time_to_fill_hours
    )

    print("\nPipeline finished.")
    return submission_results


def main():
    parser = argparse.ArgumentParser(description='Download stock data, calculate price ranges, generate and submit backtesting scenarios.')
    parser.add_argument('--ticker', type=str, required=True, help='Stock ticker symbol (e.g., AAPL)')
    parser.add_argument('--s3-key-min', type=str, required=True, help='S3 key for the minute data CSV file (e.g., path/to/TICKER-1mF.csv)')
    parser.add_argument('--group-tag', type=str, required=True, help='A tag to group related AWS Batch jobs (e.g., weekly-run-2025-04-20)')
    parser.add_argument('--output-dir', type=str, default=DEFAULT_OUTPUT_DIR, help=f'Local directory for downloads (default: {DEFAULT_OUTPUT_DIR})')
    parser.add_argument('--no-clean', action='store_true', help='Do not clean output directory before starting')
    parser.add_argument('--hold-days', type=int, default=DEFAULT_HOLD_DAYS, help=f'Fixed trade holding duration in days (default: {DEFAULT_HOLD_DAYS})')
    parser.add_argument('--fill-hours', type=int, default=DEFAULT_FILL_HOURS, help=f'Fixed time allowed for order fill in hours (default: {DEFAULT_FILL_HOURS})')
    args = parser.parse_args()

    if not re.match(r'^[a-zA-Z0-9_\-]+$', args.group_tag): print("Warning: Group tag contains characters other than letters, numbers, underscore, hyphen.")

    try:
        result = pipeline(
            ticker=args.ticker, output_dir=args.output_dir, clean_output=not args.no_clean, group_tag=args.group_tag,
            s3_key_min=args.s3_key_min, time_to_hold_days=args.hold_days, time_to_fill_hours=args.fill_hours
        )
        print("\n--- Execution Summary ---")
        print(json.dumps(result, indent=2, default=numpy_encoder))
    except ValueError as ve: print(f"\nPipeline Error: {ve}"); exit(1)
    except Exception as e: print(f"\nAn unexpected error occurred: {e}"); import traceback; traceback.print_exc(); exit(1)


if __name__ == "__main__":
    if 'MOCHI_DATA_BUCKET' not in os.environ: print("ERROR: Environment variable 'MOCHI_DATA_BUCKET' is not set."); exit(1)
    if 'AWS_DEFAULT_REGION' not in os.environ and 'AWS_REGION' not in os.environ: print("WARNING: AWS region is not explicitly set via AWS_DEFAULT_REGION or AWS_REGION environment variables.")
    main()