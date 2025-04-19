#!/usr/bin/env python3

import pandas as pd
import argparse
import sys
from datetime import timedelta


#!/usr/bin/env python3

import pandas as pd
import numpy as np
import argparse
import sys
from datetime import timedelta


#!/usr/bin/env python3

import pandas as pd
import argparse
import sys
from datetime import timedelta


def calculate_max_range(df, time_window_hours):
    """
    Calculate the maximum price range within a given time window using pandas optimized methods.

    Args:
        df (pandas.DataFrame): DataFrame containing price data with datetime index
                              and 'high' and 'low' columns.
        time_window_hours (int): The time window in hours.

    Returns:
        dict: A dictionary containing max range information
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

    # Calculate time delta in the same units as the index
    time_delta = pd.Timedelta(hours=time_window_hours)

    # Create new dataframe to store results
    result_data = []

    # For each row, find all rows within the time window and calculate range
    start_idx = 0
    total_rows = len(df)

    while start_idx < total_rows:
        start_time = df.index[start_idx]
        end_time = start_time + time_delta

        # Efficiently slice the dataframe using .loc once per window
        # This limits the amount of data we need to process
        window = df.loc[start_time:end_time]

        # Check that we have enough data and the window doesn't exceed our time limit
        if len(window) > 1 and window.index[-1] - window.index[0] <= time_delta:
            # Find highest and lowest prices in this window
            highest = window['high'].max()
            lowest = window['low'].min()
            price_range = highest - lowest

            result_data.append({
                'start_time': window.index[0],
                'end_time': window.index[-1],
                'price_range': price_range,
                'high_price': highest,
                'low_price': lowest
            })

        # Move to next row - this makes the algorithm linear time
        start_idx += 1

        # Optional: print progress for large datasets
        if start_idx % 10000 == 0:
            print(f"Processed {start_idx}/{total_rows} rows...")

    if not result_data:
        print("No valid time windows found")
        return None

    # Convert results to DataFrame for efficient operations
    results_df = pd.DataFrame(result_data)

    # Find the row with maximum price range
    max_range_row = results_df.loc[results_df['price_range'].idxmax()]

    return {
        'max_range': float(max_range_row['price_range']),
        'start_time': max_range_row['start_time'],
        'end_time': max_range_row['end_time'],
        'high_price': float(max_range_row['high_price']),
        'low_price': float(max_range_row['low_price'])
    }


def load_price_data(file_path):
    """
    Load and preprocess price data from CSV file.

    Args:
        file_path (str): Path to the CSV file containing price data.

    Returns:
        pandas.DataFrame: Preprocessed DataFrame with datetime index and price columns.
    """
    try:
        # Load the data, selecting only the required columns and parsing the 'dateTime' column
        df = pd.read_csv(file_path, usecols=['dateTime', 'high', 'low'], parse_dates=['dateTime'])

        # Rename 'dateTime' column to 'timestamp'
        df = df.rename(columns={'dateTime': 'timestamp'})

        # Set 'timestamp' as the index
        df = df.set_index('timestamp')

        # Sort by timestamp
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

    # Calculate max price range using the faster function
    result = calculate_max_range_fast(df, args.time_window)

    if result:
        print("\n===== Results =====")
        print(f"Maximum price range within {args.time_window} hours: {result['max_range']}")
        print(f"  Start time: {result['start_time']}")
        print(f"  End time: {result['end_time']}")
        print(f"  High price: {result['high_price']}")
        print(f"  Low price: {result['low_price']}")


if __name__ == "__main__":
    main()


def load_price_data(file_path):
    """
    Load and preprocess price data from CSV file.

    Args:
        file_path (str): Path to the CSV file containing price data.

    Returns:
        pandas.DataFrame: Preprocessed DataFrame with datetime index and price columns.
    """
    try:
        # Load the data, selecting only the required columns and parsing the 'dateTime' column
        df = pd.read_csv(file_path, usecols=['dateTime', 'high', 'low'], parse_dates=['dateTime'])

        # Rename 'dateTime' column to 'timestamp'
        df = df.rename(columns={'dateTime': 'timestamp'})

        # Set 'timestamp' as the index
        df = df.set_index('timestamp')

        # Sort by timestamp
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

    # Calculate max price range using the optimized function
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


def load_price_data(file_path):
    """
    Load and preprocess price data from CSV file.

    Args:
        file_path (str): Path to the CSV file containing price data.

    Returns:
        pandas.DataFrame: Preprocessed DataFrame with datetime index and price columns.
    """
    try:
        # Load the data, selecting only the required columns and parsing the 'dateTime' column
        df = pd.read_csv(file_path, usecols=['dateTime', 'high', 'low'], parse_dates=['dateTime'])

        # Rename 'dateTime' column to 'timestamp'
        df = df.rename(columns={'dateTime': 'timestamp'})

        # Set 'timestamp' as the index
        df = df.set_index('timestamp')

        # Sort by timestamp
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
