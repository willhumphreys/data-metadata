#!/usr/bin/env python3

import pandas as pd
import argparse
import sys


import pandas as pd
import numpy as np # Import numpy for NaN handling

def calculate_max_range(df, time_window_hours):
    """
    Calculate the maximum price range within a given time window using pandas optimized rolling methods.
    Excludes negative values when determining the lowest price.

    Args:
        df (pandas.DataFrame): DataFrame containing price data with datetime index
                              and 'high' and 'low' columns.
        time_window_hours (int): The time window in hours.

    Returns:
        dict: A dictionary containing max range information or None if no valid range found.
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

    # Ensure DataFrame is sorted by time (assuming index is datetime)
    if not isinstance(df.index, pd.DatetimeIndex):
        print(f"Error: DataFrame index must be a DatetimeIndex.")
        return None
    if not df.index.is_monotonic_increasing:
        print("Warning: DataFrame index is not sorted. Sorting now...")
        df = df.sort_index()

    # Define the rolling window size as a time string
    time_delta_str = f"{time_window_hours}H"
    time_delta = pd.Timedelta(hours=time_window_hours)

    # --- Performance Improvement using rolling windows ---

    # Calculate rolling maximum of 'high'
    rolling_high = df['high'].rolling(time_delta_str, closed='both').max()

    # Create a temporary series for 'low' where negative values are NaN
    # This ensures they are ignored by .min()
    positive_low = df['low'].where(df['low'] >= 0, np.nan)

    # Calculate rolling minimum of positive 'low' values
    rolling_low = positive_low.rolling(time_delta_str, closed='both').min()

    # Calculate the price range for each window
    rolling_range = rolling_high - rolling_low

    # Find the index (end time) of the window with the maximum range
    if rolling_range.empty or rolling_range.isna().all():
        print("No valid time windows found or range could not be calculated (e.g., only negative lows).")
        return None

    max_range_end_time = rolling_range.idxmax()

    # Handle potential NaT result from idxmax if all ranges are NaN
    if pd.isna(max_range_end_time):
        print("Could not determine a valid maximum range window.")
        return None

    max_range_value = rolling_range.loc[max_range_end_time]

    # Determine the start time of the best window
    # Note: The rolling window includes the endpoint, so start time calculation needs care.
    # We find the actual start by looking back from the end time.
    max_range_start_time = max_range_end_time - time_delta

    # Slice the original DataFrame for the specific window that yielded the max range
    # Ensure start time is not before the first index entry
    actual_start_time_in_df = df.index.searchsorted(max_range_start_time)
    best_window_df = df.iloc[actual_start_time_in_df:df.index.get_loc(max_range_end_time) + 1]

    # Refine window bounds based on actual data points within the theoretical window
    # Use the index values from the sliced dataframe
    actual_start_time = best_window_df.index.min()
    actual_end_time = best_window_df.index.max() # This will be <= max_range_end_time


    # Find the actual highest high and lowest positive low within THAT specific window
    actual_high_price = best_window_df['high'].max()
    positive_lows_in_window = best_window_df.loc[best_window_df['low'] >= 0, 'low']

    if positive_lows_in_window.empty:
        # This might happen in edge cases, though rolling_range should have been NaN.
        print(f"Warning: The identified best window ending at {max_range_end_time} has no positive low values.")
        # We might return None or the rolling value, depending on desired behavior.
        # Let's return None for consistency, as a valid low wasn't found in the specific window.
        return None
        # Alternatively, could use rolling_low.loc[max_range_end_time] but less precise
        # actual_low_price = rolling_low.loc[max_range_end_time]
    else:
        actual_low_price = positive_lows_in_window.min()

    # Recalculate range based on actual values in the specific window
    actual_max_range = actual_high_price - actual_low_price

    # --- End of Performance Improvement ---

    return {
        'max_range': float(actual_max_range),
        'start_time': actual_start_time, # Use the actual start time from the data in the window
        'end_time': actual_end_time,     # Use the actual end time from the data in the window
        'high_price': float(actual_high_price),
        'low_price': float(actual_low_price)
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
