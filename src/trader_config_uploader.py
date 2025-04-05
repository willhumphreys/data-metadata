import os
import boto3
import logging
from botocore.exceptions import ClientError
from datetime import datetime


def upload_file_to_s3(file_path, bucket, key, region=None):
    """
    Upload a file to an S3 bucket
    
    Args:
        file_path (str): Path to the file to upload
        bucket (str): S3 bucket name
        key (str): S3 object key
        region (str): AWS region (optional)
        
    Returns:
        bool: True if file was uploaded, else False
    """
    # Get the region from environment if not provided
    if region is None:
        region = os.environ.get('AWS_REGION', 'us-east-1')

    # Create S3 client
    s3_client = boto3.client('s3', region_name=region)

    try:
        print(f"Uploading {file_path} to S3 bucket {bucket}, key: {key}...")
        s3_client.upload_file(file_path, bucket, key)
        print(f"Upload complete. File available at s3://{bucket}/{key}")
        return True
    except ClientError as e:
        logging.error(f"Error uploading file to S3: {e}")
        print(f"Error uploading file to S3: {e}")
        return False
    except FileNotFoundError:
        logging.error(f"The file {file_path} was not found")
        print(f"The file {file_path} was not found")
        return False


def upload_trader_config(config_file_path, ticker=None, date_str=None):
    """
    Upload trader configuration file to S3 using environment variables for configuration

    Args:
        config_file_path (str): Path to the trader configuration JSON file
        ticker (str, optional): Ticker symbol to use in the S3 key
        date_str (str, optional): Date string to use in the S3 key

    Returns:
        tuple: (bool, str) - Success status and the S3 key where the file was uploaded
    """
    # Get S3 settings from environment variables
    s3_upload_bucket = os.environ.get('S3_UPLOAD_BUCKET')
    s3_upload_key_prefix = os.environ.get('S3_UPLOAD_KEY_PREFIX', 'trader-configs')
    s3_region = os.environ.get('AWS_REGION')

    # Return early if no bucket specified
    if not s3_upload_bucket:
        print("S3_UPLOAD_BUCKET environment variable not set. Skipping upload.")
        return False, None

    # Generate timestamp
    timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')

    # Use provided date or extract from timestamp
    date_str = date_str or os.environ.get('DATE_STR', timestamp.split('-')[0])

    # Create key with format: prefix/ticker/date/trader_config_timestamp.json
    filename = os.path.basename(config_file_path)
    s3_key = f"{ticker}.json"

    # Upload the file
    success = upload_file_to_s3(config_file_path, s3_upload_bucket, s3_key, s3_region)

    return success, s3_key if success else None