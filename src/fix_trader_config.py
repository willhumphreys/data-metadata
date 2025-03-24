import json
import os
import sys
from datetime import datetime


class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        import numpy as np
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        return super(NumpyEncoder, self).default(obj)


def add_config_string(config_file):
    """
    Load a trader config file, add the config_string field if missing, and save it back
    
    Args:
        config_file (str): Path to the trader configuration JSON file
    
    Returns:
        dict: The updated trader configuration
    """
    # Make sure we have the full path
    if not os.path.isabs(config_file):
        config_file = os.path.join(os.path.dirname(__file__), config_file)

    # Load the config
    try:
        with open(config_file, 'r') as f:
            config = json.load(f)
    except FileNotFoundError:
        print(f"Error: Config file {config_file} not found")
        return None
    except json.JSONDecodeError:
        print(f"Error: Config file {config_file} is not valid JSON")
        return None

    # Add config_string if missing
    if 'config_string' not in config:
        # Create a simple string representation of the config
        config_str_parts = []

        # Add important fields to the config string
        if 'ticker' in config:
            config_str_parts.append(f"ticker={config['ticker']}")

        if 'timestamp' in config:
            config_str_parts.append(f"timestamp={config['timestamp']}")
        else:
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            config_str_parts.append(f"timestamp={timestamp}")
            config['timestamp'] = timestamp

        # Add any other important fields
        for key in ['strategy', 'period', 'threshold']:
            if key in config:
                config_str_parts.append(f"{key}={config[key]}")

        # Join all parts with commas
        config['config_string'] = ", ".join(config_str_parts)

        # Save the updated config back to the file
        with open(config_file, 'w') as f:
            json.dump(config, f, cls=NumpyEncoder, indent=2)

        print(f"Added config_string to {config_file}: {config['config_string']}")

    return config


if __name__ == "__main__":
    # If run directly, use the first argument as the config file path
    if len(sys.argv) > 1:
        add_config_string(sys.argv[1])
    else:
        print("Usage: python fix_trader_config.py <config_file>")