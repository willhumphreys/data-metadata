import pandas as pd
from datetime import timedelta


def calculate_max_range(
        dataframe,
        time_window_hours=8,
        datetime_column='dateTime',
        high_column='high',
        low_column='low'
):
    """
    Calculate the maximum range between high and low prices within a specified time window.

    Args:
        dataframe (pd.DataFrame): DataFrame containing the price data
        time_window_hours (int): Maximum time window in hours to consider for the range calculation
        datetime_column (str): Name of the column containing the datetime information
        high_column (str): Name of the column containing the high prices
        low_column (str): Name of the column containing the low prices

    Returns:
        dict: A dictionary containing:
            - max_range: The maximum price range found
            - start_time: The start time of the maximum range
            - end_time: The end time of the maximum range
            - start_price: The price at the start of the range
            - end_price: The price at the end of the range
    """
    # Ensure datetime column is in datetime format
    df = dataframe.copy()
    if not pd.api.types.is_datetime64_any_dtype(df[datetime_column]):
        df[datetime_column] = pd.to_datetime(df[datetime_column])

    # Sort by datetime to ensure correct window calculation
    df = df.sort_values(by=datetime_column)

    max_range = 0
    max_range_data = {
        'max_range': 0,
        'start_time': None,
        'end_time': None,
        'high_price': None,
        'low_price': None
    }

    # Convert time_window_hours to timedelta
    time_window = timedelta(hours=time_window_hours)

    # Iterate through each row as a potential starting point
    for i, start_row in df.iterrows():
        start_time = start_row[datetime_column]
        end_time = start_time + time_window

        # Get all prices within the time window
        window_data = df[(df[datetime_column] >= start_time) &
                         (df[datetime_column] <= end_time)]

        if not window_data.empty:
            # Find the maximum high and minimum low in the window
            window_max_high = window_data[high_column].max()
            window_min_low = window_data[low_column].min()

            # Calculate the range
            range_value = window_max_high - window_min_low

            # Update max range if this window has a larger range
            if range_value > max_range:
                max_range = range_value
                max_range_data = {
                    'max_range': range_value,
                    'start_time': start_time,
                    'end_time': window_data[datetime_column].max(),
                    'high_price': window_max_high,
                    'low_price': window_min_low
                }

    return max_range_data


def load_price_data(file_path):
    """
    Load price data from CSV file.

    Args:
        file_path (str): Path to the CSV file

    Returns:
        pd.DataFrame: DataFrame containing the price data
    """
    df = pd.read_csv(file_path)
    return df


def main(file_path, time_window_hours=8):
    """
    Main function to calculate the maximum price range.

    Args:
        file_path (str): Path to the CSV file containing price data
        time_window_hours (int): Maximum time window in hours to consider for the range calculation

    Returns:
        dict: Result of the max range calculation
    """
    df = load_price_data(file_path)
    result = calculate_max_range(df, time_window_hours)

    print(f"Maximum price range within {time_window_hours} hours: {result['max_range']}")
    print(f"Start time: {result['start_time']}")
    print(f"End time: {result['end_time']}")
    print(f"High price: {result['high_price']}")
    print(f"Low price: {result['low_price']}")

    return result


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python price_range_calculator.py <file_path> [time_window_hours]")
        sys.exit(1)

    file_path = sys.argv[1]
    time_window_hours = int(sys.argv[2]) if len(sys.argv) > 2 else 8

    main(file_path, time_window_hours)
