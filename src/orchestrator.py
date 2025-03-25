#!/usr/bin/env python3

import shutil
import argparse
from datetime import datetime, timedelta

# Import functions from our other modules
from s3_downloader import download_from_s3, DEFAULT_S3_KEY_PATH, DEFAULT_OUTPUT_DIR
from price_range_calculator import calculate_max_range, load_price_data
from src.trader_config_uploader import upload_trader_config


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



def pipeline(
        ticker='AAPL',
        s3_key_path=DEFAULT_S3_KEY_PATH,
        output_dir=DEFAULT_OUTPUT_DIR,
        time_window_hours=[8, 336],  # Default: 8 hours and 14 days (336 hours)
        date=None,
        clean_output=True,
        target_combinations=100000  # Target number of trader combinations
):
    """
    Execute the full data pipeline: download from S3, analyze price ranges, generate trader configs.

    Args:
        ticker (str): The stock ticker symbol (e.g., 'AAPL')
        s3_key_path (str): S3 key path template
        output_dir (str): Directory to save downloaded and processed files
        time_window_hours (list): Time windows in hours for price range calculation
        date (str): Specific date to analyze (format: YYYY-MM-DD), defaults to yesterday
        clean_output (bool): Whether to clean the output directory before starting
        target_combinations (int): Target number of trader combinations to generate

    Returns:
        dict: Results including price range analysis and trader configurations
    """
    # Initialize results dict
    results = {}

    if clean_output:
        print(f"Cleaning output directory: {output_dir}")
        clean_directory(output_dir)

    # If date is not provided, use yesterday's date
    if not date:
        yesterday = datetime.now() - timedelta(days=1)
        date = yesterday.strftime('%Y-%m-%d')

    print(f"\n===== Processing {ticker} data for {date} =====\n")

    # Step 1: Download data from S3
    print(f"[1/2] Downloading {ticker} data from S3...")
    file_path = download_from_s3(s3_key_path, output_dir)

    if not file_path:
        print(f"Error: Failed to download data for {ticker} on {date}")
        return results

    # Step 2: Calculate maximum price ranges
    print(f"\n[2/2] Calculating maximum price ranges...")
    df = load_price_data(file_path, ticker)

    if df.empty:
        print(f"Error: No valid price data loaded for {ticker}")
        return results

    # Calculate price ranges for the specified time windows
    for hours in time_window_hours:
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
        results[f"{hours}_hours"] = result

    # Generate trader configurations if we have both 8-hour and 14-day results
    if "8_hours" in results and "336_hours" in results:
        trader_config = generate_trader_configs(results["8_hours"], results["336_hours"], ticker, target_combinations)

        # Add trader configurations to results
        results['trader_config'] = trader_config

        # Print trader configuration details
        print(f"\n===== Trader Configuration =====")
        # print(f"Config string: {trader_config['config']}")
        print(f"Total combinations: {trader_config['total_combinations']:,}")
        print(f"\nParameter ranges:")
        print(
            f"  Stops: {trader_config['parameters']['stops']['min']} to {trader_config['parameters']['stops']['max']} in steps of {trader_config['parameters']['stops']['step']} ({trader_config['parameters']['stops']['count']} values)")
        print(
            f"  Limits: {trader_config['parameters']['limits']['min']} to {trader_config['parameters']['limits']['max']} in steps of {trader_config['parameters']['limits']['step']} ({trader_config['parameters']['limits']['count']} values)")
        print(
            f"  Offsets: {trader_config['parameters']['offsets']['min']} to {trader_config['parameters']['offsets']['max']} in steps of {trader_config['parameters']['offsets']['step']} ({trader_config['parameters']['offsets']['count']} values)")

    return results


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


def generate_trader_configs(eight_hour_results, fourteen_day_results, ticker, target_combinations=100000,
                            output_file="trader_config.json"):
    """
    Generate trader configuration strings based on price range analysis results and save to JSON file.

    Args:
        eight_hour_results (dict): Results from 8-hour window analysis
        fourteen_day_results (dict): Results from 14-day window analysis
        target_combinations (int): Target number of trader combinations
        output_file (str): Path to output JSON file

    Returns:
        dict: Trader configuration details
        :param output_file:
        :param eight_hour_results:
        :param fourteen_day_results:
        :param target_combinations:
        :param ticker:
    """
    # Extract the price ranges - convert numpy types to native Python types if needed
    short_term_range = int(numpy_encoder(eight_hour_results['max_range']))  # 1750
    long_term_range = int(numpy_encoder(fourteen_day_results['max_range']))  # 3561

    # Calculate offset parameters to cover the entire short-term range
    offset_min = -short_term_range - 1  # -1751
    offset_max = short_term_range + 1  # 1751

    # Set stop and limit to just beyond the observed maximum range
    stop_min = -long_term_range - 1  # -3562
    stop_max = -1  # Minimum meaningful stop loss

    limit_min = 1  # Minimum meaningful take profit
    limit_max = long_term_range + 1  # 3562

    # Fixed values for duration and output
    duration_min = 14
    duration_max = 14
    duration_step = 7

    output_min = 8
    output_max = 8
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
    scenario = f"s_{stop_min}..{stop_max}..{stop_step}___" + \
                    f"l_{limit_min}..{limit_max}..{limit_step}___" + \
                    f"o_{offset_min}..{offset_max}..{offset_step}___" + \
                    f"d_{duration_min}..{duration_max}..{duration_step}___" + \
                    f"out_{output_min}..{output_max}..{output_step}"

    # Create result dictionary
    result = {
        'scenario': scenario,
        'total_combinations': total_combinations,
        'parameters': {
            'stops': {'min': stop_min, 'max': stop_max, 'step': stop_step, 'count': num_stops},
            'limits': {'min': limit_min, 'max': limit_max, 'step': limit_step, 'count': num_limits},
            'offsets': {'min': offset_min, 'max': offset_max, 'step': offset_step, 'count': num_offsets},
            'durations': {'min': duration_min, 'max': duration_max, 'step': duration_step, 'count': num_durations},
            'outputs': {'min': output_min, 'max': output_max, 'step': output_step, 'count': num_outputs}
        },
        'price_analysis': {
            'short_term': {
                'max_range': short_term_range,
                'window_hours': 8
            },
            'long_term': {
                'max_range': long_term_range,
                'window_hours': 336  # 14 days
            }
        }
    }

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
    success, s3_key = upload_trader_config(output_file, ticker)

    if success:
        # Add S3 information to the result
        result['s3_info'] = {
            'uploaded': True,
            'bucket': os.environ.get('S3_UPLOAD_BUCKET'),
            'key': s3_key
        }

    return result


def main():
    """
    Parse command line arguments and execute the pipeline.
    """
    parser = argparse.ArgumentParser(description='Download stock data and calculate maximum price ranges.')
    parser.add_argument('--ticker', type=str, default='AAPL', help='Stock ticker symbol')
    parser.add_argument('--date', type=str, help='Date to analyze (YYYY-MM-DD), defaults to yesterday')
    parser.add_argument('--time-windows', type=str, default="8,336",
                        help='Comma-separated list of time windows in hours (default: 8,336)')
    parser.add_argument('--s3-path', type=str, default=DEFAULT_S3_KEY_PATH, help='S3 key path template')
    parser.add_argument('--output-dir', type=str, default=DEFAULT_OUTPUT_DIR, help='Output directory')
    parser.add_argument('--no-clean', action='store_true', help='Do not clean output directory before starting')

    args = parser.parse_args()

    # Parse the time windows from the comma-separated string
    time_windows = [int(hours.strip()) for hours in args.time_windows.split(',')]

    result = pipeline(
        ticker=args.ticker,
        s3_key_path=args.s3_path,
        output_dir=args.output_dir,
        time_window_hours=time_windows,
        date=args.date,
        clean_output=not args.no_clean
    )

    return result

if __name__ == "__main__":

    main()
