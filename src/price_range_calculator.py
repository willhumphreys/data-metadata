#!/usr/bin/env python3

import pandas as pd
import argparse
import sys
from datetime import timedelta


def calculate_max_range(df, time_window_hours):
    """
    Calculate the maximum price range within a given time window.

    Args:
        df (pandas.DataFrame): DataFrame containing price data with datetime index
                              and 'high' and 'low' columns.
        time_window_hours (int): The time window in hours.

    Returns:
        dict: A dictionary containing:
            - max_range: The maximum price range
            - start_time: The start time of the window with maximum range
            - end_time: The end time of the window with maximum range
            - high_price: The highest price in the window
            - low_price: The lowest price in the window
    """
    if df.empty:
        print("Error: DataFrame is empty")
        return None

    # Check if required columns exist
    required_columns = ['high', 'low']
    for col in required_columns:
        if col not in df.columns:
            print(f"Error: Required column '{col}' not found in data")
            return None

    # Ensure DataFrame is sorted by time
    df = df.sort_index()

    max_range = 0
    result = {
        'max_range': 0,
        'start_time': None,
        'end_time': None,
        'high_price': 0,
        'low_price': 0
    }

    # Calculate the time delta for our window
    time_delta = timedelta(hours=time_window_hours)

    # Iterate through each timestamp as a potential starting point
    for i in range(len(df)):
        start_time = df.index[i]
        end_time = start_time + time_delta

        # Get all rows within our time window
        window = df.loc[start_time:end_time]

        # Skip if window is too short (could be at the end of the dataset)
        if len(window) <= 1:
            continue

        # Check that the window doesn't exceed our time limit
        actual_duration = window.index[-1] - window.index[0]
        if actual_duration > time_delta:
            continue

        # Find highest and lowest prices in this window
        highest = window['high'].max()
        lowest = window['low'].min()

        # Calculate range
        price_range = highest - lowest

        # Update if this range is greater than the max seen so far
        if price_range > max_range:
            max_range = price_range
            result = {
                'max_range': max_range,
                'start_time': window.index[0],
                'end_time': window.index[-1],
                'high_price': highest,
                'low_price': lowest
            }

    return result


def load_price_data(file_path):
    """
    Load and preprocess price data from CSV file.

    Args:
        file_path (str): Path to the CSV file containing price data.

    Returns:
        pandas.DataFrame: Preprocessed DataFrame with datetime index and price columns.
    """
    try:
        # Load the data
        df = pd.read_csv(file_path)

        # Check if required columns exist
        required_columns = ['timestamp', 'high', 'low']
        missing_columns = [col for col in required_columns if col not in df.columns]

        if missing_columns:
            print(f"Error: Missing required columns: {', '.join(missing_columns)}")
            return pd.DataFrame()

        # Convert timestamp from milliseconds to datetime
        df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')

        # Set datetime as the index
        df = df.set_index('datetime')

        # Convert price columns to integers (scale by multiplying by 100)
        price_columns = ['open', 'high', 'low', 'close', 'vwap']
        for col in price_columns:
            if col in df.columns:
                # Multiply by 100 and round to nearest integer
                df[col] = (df[col] * 100).round().astype(int)

        # Sort by datetime
        df = df.sort_index()

        print(f"Loaded {len(df)} rows of price data")
        return df

    except Exception as e:
        print(f"Error loading price data: {str(e)}")
        return pd.DataFrame()


def main():
    """
    Parse command line arguments and execute the price range calculation.
    """
    parser = argparse.ArgumentParser(description='Calculate maximum price range within a time window.')
    parser.add_argument('file_path', help='Path to the CSV file containing price data')
    parser.add_argument('--time-window', type=int, default=8,
                        help='Time window in hours for price range calculation')

    args = parser.parse_args()

    # Load the price data
    df = load_price_data(args.file_path)

    if df.empty:
        sys.exit("Error: No data available for analysis")

    # Calculate max price range
    result = calculate_max_range(df, args.time_window)

    if result:
        print("\n===== Results =====")
        print(f"Maximum price range within {args.time_window} hours: {result['max_range']}")
        print(f"  Start time: {result['start_time']}")
        print(f"  End time: {result['end_time']}")
        print(f"  High price: {result['high_price']}")
        print(f"  Low price: {result['low_price']}")


if __name__ == "__main__":
    main()
