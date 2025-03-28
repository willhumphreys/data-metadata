#!/usr/bin/env python3

import argparse
import shutil
import boto3

from price_range_calculator import calculate_max_range, load_price_data
from s3_downloader import download_from_s3, DEFAULT_OUTPUT_DIR
from job_placer import put_trade_job_on_queue
from models import BatchParameters


def clean_directory(directory):
    """
    Clean (empty) the specified directory by removing all files and subdirectories.
    Creates the directory if it doesn't exist.

    Args:
        directory (str): Path to the directory to clean
    """
    if os.path.exists(directory):
        for item in os.listdir(directory):
            item_path = os.path.join(directory, item)
            if os.path.isfile(item_path):
                os.unlink(item_path)
            elif os.path.isdir(item_path):
                shutil.rmtree(item_path)
    else:
        os.makedirs(directory)


def pipeline(ticker=None, s3_key_path=None, output_dir=DEFAULT_OUTPUT_DIR, clean_output=True,
             target_combinations=100000, group_tag=None, s3_key_min=None):

    if clean_output:
        print(f"Cleaning output directory: {output_dir}")
        clean_directory(output_dir)

    file_path = download_from_s3(s3_key_path, output_dir)

    if not file_path:
        print(f"Error: Failed to download data for {s3_key_path}")
        raise ValueError("Failed to download data")

    print(f"\n[2/2] Calculating maximum price ranges...")
    df = load_price_data(file_path)

    if df.empty:
        print(f"Error: No valid price data loaded for {ticker}")
        raise ValueError("No valid price data loaded")

    results = {}

    add_results(df, 8, "time_to_place", results)
    add_results(df, 336, "time_to_hold", results)

    trader_config = place_trade_jobs(results["time_to_place"], results["time_to_hold"], ticker, s3_key_min,
                                     target_combinations, group_tag=group_tag, time_to_hold=14, time_to_fill=8)

    results28Days = {}

    add_results(df, 8, "time_to_place", results28Days)
    add_results(df, 336 * 2, "time_to_hold", results28Days)

    trader_config = place_trade_jobs(results28Days["time_to_place"], results28Days["time_to_hold"], ticker, s3_key_min,
                                 target_combinations, group_tag=group_tag, time_to_hold=28, time_to_fill=8)

    results42Days = {}

    add_results(df, 8, "time_to_place", results42Days)
    add_results(df, 336 * 3, "time_to_hold", results42Days)

    trader_config = place_trade_jobs(results42Days["time_to_place"], results42Days["time_to_hold"], ticker, s3_key_min,
                                 target_combinations, group_tag=group_tag, time_to_hold=42, time_to_fill=8)

    combined_results = {
        "14days": results,
        "28days": results28Days,
        "42days": results42Days
    }

    return combined_results



def add_results(df, hours, text, results):
    print(f"\nAnalyzing {hours} hour{'s' if hours != 1 else ''} window...")
    result = calculate_max_range(df, hours)
    # Convert hours to days for display if applicable
    if hours >= 24:
        days = hours / 24
        time_desc = f"{days:.1f} days ({hours} hours)"
    else:
        time_desc = f"{hours} hours"
    print(f"\n===== Results for {time_desc} =====")
    print(f"Maximum price range: {result['max_range']}")
    print(f"  Start time: {result['start_time']}")
    print(f"  End time: {result['end_time']}")
    print(f"  High price: {result['high_price']}")
    print(f"  Low price: {result['low_price']}")
    # Store result with a descriptive key
    results[f"{text}"] = result


import json
import os
import numpy as np  # Add this import


def numpy_encoder(obj):
    """
    Helper function to convert numpy types to Python native types for JSON serialization.
    """
    if isinstance(obj, (np.integer, np.int64, np.int32)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float64, np.float32)):
        return float(obj)
    elif isinstance(obj, (np.ndarray,)):
        return obj.tolist()
    elif isinstance(obj, np.bool_):
        return bool(obj)
    else:
        return obj

def create_batch_parameters(group_tag, scenario, ticker, trade_type="long", s3_key_min=None):
    # Format ticker for scenario
    symbol_file = f"{ticker}-1mF.csv"
    base_symbol = ticker
    # Build the full scenario string

    if trade_type == "long":
        full_scenario = f"{scenario}___{symbol_file}"
    else:
        full_scenario = f"{scenario}___short"

    # Generate job names
    trades_job_name = f"Trades{ticker}-{group_tag}"
    aggregate_job_name = f"Aggregate{ticker}-{group_tag}-{trade_type}"
    graphs_job_name = f"Graphs{ticker}-"
    # trades_job_name_short = f"Trades{ticker}-{group_tag}-short"
    aggregate_job_name_short = f"Aggregate{ticker}-{group_tag}-{trade_type}"
    graphs_job_name = f"Graphs{ticker}-{trade_type}-"
    print(f"Submitting job with name: {trades_job_name} with scenario: {full_scenario}")
    # Common job queue
    queue_name = "fargateSpotTrades"
    print(f"Using queue: {queue_name}")
    return BatchParameters(aggregate_job_name=aggregate_job_name, base_symbol=base_symbol, full_scenario=full_scenario,
        graphs_job_name=graphs_job_name, queue_name=queue_name, scenario=scenario, symbol_file=symbol_file,
        trade_type=trade_type, trades_job_name=trades_job_name, group_tag=group_tag, s3_key_min=s3_key_min)


def place_trade_jobs(time_to_place_results: dict, time_to_hold_results: dict, ticker: str, s3_key_min: str,
                     target_combinations: int = 100000, output_file: str = "trader_config.json", group_tag: str = None,
                     time_to_hold=None, time_to_fill=None, ) -> dict:
    # Extract the price ranges - convert numpy types to native Python types if needed
    time_to_place_range = int(numpy_encoder(time_to_place_results['max_range']))  # 1750
    time_to_hold_range = int(numpy_encoder(time_to_hold_results['max_range']))  # 3561

    # Calculate offset parameters to cover the entire short-term range
    offset_min = -time_to_place_range - 1  # -1751
    offset_max = time_to_place_range + 1  # 1751

    # Set stop and limit to just beyond the observed maximum range
    stop_min = -time_to_hold_range - 1  # -3562
    stop_max = -1  # Minimum meaningful stop loss

    limit_min = 1  # Minimum meaningful take profit
    limit_max = time_to_hold_range + 1  # 3562

    # Fixed values for duration and output
    duration_min = time_to_hold
    duration_max = time_to_hold
    duration_step = 7

    output_min = time_to_fill
    output_max = time_to_fill
    output_step = 4

    # Calculate steps to get close to target_combinations
    target_per_variable = int(target_combinations ** (1 / 3))

    # Calculate steps
    stop_range = stop_max - stop_min
    stop_step = max(1, int(stop_range / target_per_variable))

    limit_range = limit_max - limit_min
    limit_step = max(1, int(limit_range / target_per_variable))

    offset_range = offset_max - offset_min
    offset_step = max(1, int(offset_range / target_per_variable))

    # Calculate number of combinations
    num_stops = 1 + (stop_max - stop_min) // stop_step if stop_step > 0 else 1
    num_limits = 1 + (limit_max - limit_min) // limit_step if limit_step > 0 else 1
    num_offsets = 1 + (offset_max - offset_min) // offset_step if offset_step > 0 else 1
    num_durations = 1  # Fixed
    num_outputs = 1  # Fixed

    total_combinations = num_stops * num_limits * num_offsets * num_durations * num_outputs

    # Fine-tune to get closer to target combinations
    if abs(total_combinations - target_combinations) > (target_combinations * 0.1):
        adjustment = (target_combinations / total_combinations) ** (1 / 3)

        stop_step = max(1, int(stop_step / adjustment))
        limit_step = max(1, int(limit_step / adjustment))
        offset_step = max(1, int(offset_step / adjustment))

        num_stops = 1 + (stop_max - stop_min) // stop_step if stop_step > 0 else 1
        num_limits = 1 + (limit_max - limit_min) // limit_step if limit_step > 0 else 1
        num_offsets = 1 + (offset_max - offset_min) // offset_step if offset_step > 0 else 1

        total_combinations = num_stops * num_limits * num_offsets * num_durations * num_outputs

    # Generate the configuration string
    scenario = f"s_{stop_min}..{stop_max}..{stop_step}___" + f"l_{limit_min}..{limit_max}..{limit_step}___" + f"o_{offset_min}..{offset_max}..{offset_step}___" + f"d_{duration_min}..{duration_max}..{duration_step}___" + f"out_{output_min}..{output_max}..{output_step}"

    # Create result dictionary
    result = {'scenario': scenario, 'total_combinations': total_combinations,
              'parameters': {'stops': {'min': stop_min, 'max': stop_max, 'step': stop_step, 'count': num_stops},
                             'limits': {'min': limit_min, 'max': limit_max, 'step': limit_step, 'count': num_limits},
                             'offsets': {'min': offset_min, 'max': offset_max, 'step': offset_step,
                                         'count': num_offsets},
                             'durations': {'min': duration_min, 'max': duration_max, 'step': duration_step,
                                           'count': num_durations},
                             'outputs': {'min': output_min, 'max': output_max, 'step': output_step,
                                         'count': num_outputs}},
              'price_analysis': {'short_term': {'max_range': time_to_place_range, 'window_hours': 8},
                                 'long_term': {'max_range': time_to_hold_range, 'window_hours': 336  # 14 days
                                               }}}

    # Process the entire dictionary to ensure all values are JSON serializable
    def convert_dict_values(d):
        if isinstance(d, dict):
            return {k: convert_dict_values(v) for k, v in d.items()}
        elif isinstance(d, list):
            return [convert_dict_values(item) for item in d]
        else:
            return numpy_encoder(d)

    result = convert_dict_values(result)

    # Write to JSON file
    with open(output_file, 'w') as f:
        json.dump(result, f, indent=2)

    print(f"Trader configuration saved to {os.path.abspath(output_file)}")

    # Upload to S3 if environment variables are set
    # success, s3_key = upload_trader_config(output_file, ticker)

    batch_params_long = create_batch_parameters(group_tag, scenario, ticker, "long", s3_key_min)
    batch_params_short = create_batch_parameters(group_tag, scenario, ticker, "short", s3_key_min)

    # Initialize boto3 client
    batch_client = boto3.client('batch')

    put_trade_job_on_queue(batch_params_long, batch_client)

    put_trade_job_on_queue(batch_params_short, batch_client)

    return {'statusCode': 200, 'body': json.dumps({'message': f'Successfully calculated new scenarios for {ticker}'})}


def main():
    """
    Parse command line arguments and execute the pipeline.
    """
    parser = argparse.ArgumentParser(description='Download stock data and calculate maximum price ranges.')
    parser.add_argument('--ticker', type=str, default='AAPL', help='Stock ticker symbol')
    parser.add_argument('--date', type=str, help='Date to analyze (YYYY-MM-DD), defaults to yesterday')
    # parser.add_argument('--time-windows', type=str, default="8,336",
    #                     help='Comma-separated list of time windows in hours (default: 8,336)')
    parser.add_argument('--s3-key-hour', type=str, help='S3 key hour path template')
    parser.add_argument('--s3-key-min', type=str, help='S3 key minute path template')
    parser.add_argument('--output-dir', type=str, default=DEFAULT_OUTPUT_DIR, help='Output directory')
    parser.add_argument('--no-clean', action='store_true', help='Do not clean output directory before starting')
    parser.add_argument('--group-tag', type=str, default='NotSet', help='The group tag to use for this run')

    args = parser.parse_args()

    result = pipeline(ticker=args.ticker, s3_key_path=args.s3_key_hour, output_dir=args.output_dir,
                      clean_output=not args.no_clean, group_tag=args.group_tag,
                      s3_key_min=args.s3_key_min)

    return result


if __name__ == "__main__":
    main()
