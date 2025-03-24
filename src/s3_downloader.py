import os
import sys
import boto3
import subprocess
from dotenv import load_dotenv

# Default S3 key path - can be used when no command line argument is provided

DEFAULT_S3_KEY_PATH = "stocks/AAPL/polygon/2025/03/23/22/AAPL_polygon_hour.csv.lzo"
# Default output directory
DEFAULT_OUTPUT_DIR = "../output"


def download_from_s3(key_path=None, output_dir=None):
    """
    Download a file from S3 using environment variables for region and bucket
    and a provided key path.

    Args:
        key_path (str, optional): The path to the file within the S3 bucket.
                                  If not provided, uses DEFAULT_S3_KEY_PATH.
        output_dir (str, optional): Directory to save downloaded files.
                                   If not provided, uses DEFAULT_OUTPUT_DIR.

    Returns:
        str: Path to the downloaded file (or the decompressed CSV file if LZO)
    """
    # If no key_path provided, use default
    if key_path is None:
        key_path = DEFAULT_S3_KEY_PATH
        print(f"No S3 key path provided. Using default: {key_path}")

    # If no output_dir provided, use default
    if output_dir is None:
        output_dir = DEFAULT_OUTPUT_DIR
        print(f"No output directory provided. Using default: {output_dir}")

    # Create the output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Load environment variables from .env file if it exists
    load_dotenv()

    # Get the region and bucket from environment variables
    region = os.environ.get('AWS_REGION')
    bucket = os.environ.get('S3_BUCKET')

    # Validate environment variables
    if not region:
        sys.exit("Error: AWS_REGION environment variable not set")
    if not bucket:
        sys.exit("Error: S3_BUCKET environment variable not set")

    # Extract the filename from the key path
    filename = os.path.basename(key_path)
    output_path = os.path.join(output_dir, filename)

    print(f"Downloading from bucket: {bucket}")
    print(f"Using region: {region}")
    print(f"Key path: {key_path}")
    print(f"Output path: {output_path}")

    try:
        # Initialize S3 client
        s3_client = boto3.client('s3', region_name=region)

        # Download the file
        print(f"Downloading file...")
        s3_client.download_file(bucket, key_path, output_path)

        print(f"Download complete. File saved as: {output_path}")

        # Check if file is an LZO file and decompress it
        if output_path.endswith('.lzo'):
            csv_path = decompress_lzo(output_path, output_dir)
            return csv_path  # Return the CSV path instead of the LZO path

        return output_path  # Return the original file path if not LZO

    except Exception as e:
        sys.exit(f"Error downloading file: {e}")


def decompress_lzo(lzo_filepath, output_dir=None):
    """
    Decompress an LZO file using the lzop command-line tool.

    Args:
        lzo_filepath (str): The path to the LZO file to decompress
        output_dir (str, optional): Directory to save the decompressed file.
                                   If None, use the same directory as the LZO file.
    """
    # Check if lzop is installed
    try:
        # Run 'which lzop' to check if lzop is available
        subprocess.run(['which', 'lzop'], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except subprocess.CalledProcessError:
        print("Warning: lzop command not found. Please install lzop to decompress LZO files.")
        print("You can install it with: sudo apt-get install lzop (Ubuntu/Debian)")
        print("Or: brew install lzop (macOS with Homebrew)")
        return None

    # Get the base filename (without .lzo extension)
    if lzo_filepath.endswith('.lzo'):
        output_filename = os.path.basename(lzo_filepath)[:-4]
    else:
        output_filename = f"{os.path.basename(lzo_filepath)}_decompressed"

    # Create full output path
    if output_dir is None:
        output_dir = os.path.dirname(lzo_filepath)

    output_path = os.path.join(output_dir, output_filename)

    try:
        print(f"Decompressing {lzo_filepath}...")
        # Run the lzop command to decompress
        # -d: decompress
        # -o: specify output file
        subprocess.run(['lzop', '-d', '-o', output_path, lzo_filepath], check=True)
        print(f"Decompression complete. File saved as: {output_path}")
        return output_path
    except subprocess.CalledProcessError as e:
        print(f"Error decompressing file: {e}")
        print("If you're receiving 'command not found', please install lzop.")
        return None


# Direct function call for easier usage in IntelliJ (can run file directly)
def main():
    """
    Main function that can be called directly when running the file
    with no arguments in IntelliJ.
    """
    return download_from_s3()


if __name__ == "__main__":
    # Parse command line arguments
    if len(sys.argv) == 1:
        # No arguments, use defaults
        main()
    elif len(sys.argv) == 2:
        # Only S3 key path provided
        download_from_s3(sys.argv[1])
    elif len(sys.argv) == 3:
        # Both S3 key path and output directory provided
        download_from_s3(sys.argv[1], sys.argv[2])
    else:
        sys.exit("Usage: python s3_downloader.py [s3_key_path] [output_directory]")
