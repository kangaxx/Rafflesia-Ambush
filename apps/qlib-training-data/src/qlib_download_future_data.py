"""
Qlib Futures Data Downloader

This script provides a command-line interface for downloading futures data using qlib.
It supports customizable parameters for output path, data frequency, and contract codes.
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import List, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Try to import qlib

try:
    import qlib
    from qlib.data import D
    from qlib.data.data import Calender
    from qlib.data.instrument import Instrument
    import pandas as pd
except ImportError:
    logger.error("Failed to import qlib. Please install it with: pip install pyqlib")
    sys.exit(1)


def setup_argparse() -> argparse.ArgumentParser:
    """
    Set up command-line argument parser for futures data downloading.
    
    Returns:
        argparse.ArgumentParser: Configured argument parser
    """
    parser = argparse.ArgumentParser(
        description="Qlib Futures Data Downloader\n"
        "====================================\n"
        "This script provides a comprehensive tool for downloading futures data using Qlib.\n"
        "It supports customizable parameters for data storage, frequency, contract selection,\n"
        "date ranges, and includes advanced features like download resumption.\n"
        "\n"
        "Example usage:\n"
        "  python qlib_download_future_data.py --contracts CU,AL,ZN\n"
        "  python qlib_download_future_data.py --contracts IF,IC --freq 5min --start-date 2023-01-01\n"
        "  python qlib_download_future_data.py --contracts RU,BU --resume --chunk-size 15",
        add_help=False  # 禁用默认的帮助参数，以便我们自定义
    )
    
    # 添加自定义的帮助参数，包括--h和--help
    parser.add_argument(
        '-h', '--help',
        action='help',
        help='Show this help message and exit'
    )
    
    # Data storage configuration
    parser.add_argument(
        '--output-path',
        type=str,
        default='~/.qlib/qlib_data/cn_future',
        help='Path to save the downloaded futures data. Supports tilde (~) for home directory.'
    )
    
    # Contract specifications
    parser.add_argument(
        '--contracts',
        type=str,
        required=True,
        help='Future contracts to download, separated by commas (e.g., "CU,AL,ZN").\n'
        'The script will automatically find all valid contracts for each code.\n'
        'For example, specifying "CU" will download all copper futures contracts.'
    )
    
    # Data frequency
    parser.add_argument(
        '--freq',
        type=str,
        default='1d',
        choices=['1min', '5min', '15min', '30min', '60min', '1d'],
        help='Data frequency for the downloaded futures data.\n'
        'Supports: 1min, 5min, 15min, 30min, 60min (hourly), or 1d (daily).\n'
        'Intraday frequencies provide more granular data but may result in larger file sizes.'
    )
    
    # Date range
    parser.add_argument(
        '--start-date',
        type=str,
        help='Start date for data download in YYYY-MM-DD format.\n'
        'If not specified, the earliest available data will be downloaded.\n'
        'Example: --start-date 2022-01-01'
    )
    
    parser.add_argument(
        '--end-date',
        type=str,
        help='End date for data download in YYYY-MM-DD format.\n'
        'If not specified, the latest available data will be downloaded.\n'
        'Example: --end-date 2023-12-31'
    )
    
    # Data fields
    parser.add_argument(
        '--fields',
        type=str,
        default='open,high,low,close,volume,amount',
        help='Data fields to download, separated by commas.\n'
        'Common fields include: open, high, low, close, volume, amount.\n'
        'You can specify a subset of fields to reduce file size if needed.'
    )
    
    # Region
    parser.add_argument(
        '--region',
        type=str,
        default='cn',
        choices=['cn', 'us'],
        help='Data region to download from. Default is China (cn).\n'
        'Note: Contract availability depends on the selected region.'
    )
    
    # Logging level
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose logging for more detailed output during download.'
    )
    
    # Resume download
    parser.add_argument(
        '--resume',
        action='store_true',
        help='Resume download from previous checkpoint if available.\n'
        'This feature is useful for large downloads that might get interrupted.\n'
        'The script will check for existing data files and continue from the latest date.'
    )
    
    # Download chunk size (days)
    parser.add_argument(
        '--chunk-size',
        type=int,
        default=30,
        help='Number of days to download in each chunk (default: 30).\n'
        'Smaller chunk sizes are useful for unstable connections or large date ranges.\n'
        'Larger chunk sizes may be more efficient for stable connections.'
    )
    
    return parser


def initialize_qlib(provider_uri: str, region: str) -> bool:
    """
    Initialize Qlib with the specified configuration settings.
    
    This function prepares the Qlib environment by:
    1. Expanding the user home directory if the path contains '~'
    2. Creating the necessary directories if they don't exist
    3. Initializing Qlib with the specified provider URI and region
    
    Args:
        provider_uri: Path to store Qlib data. Supports tilde (~) notation.
        region: Data region to use, either 'cn' (China) or 'us' (United States)
        
    Returns:
        bool: True if initialization was successful, False otherwise
        
    Raises:
        Any exceptions during initialization will be caught and logged
    """
    try:
        # Expand user home directory if present
        provider_uri = str(Path(provider_uri).expanduser())
        
        # Ensure the directory exists
        Path(provider_uri).mkdir(parents=True, exist_ok=True)
        
        qlib.init(provider_uri=provider_uri, region=region)
        logger.info(f"Qlib initialized successfully with region: {region}")
        logger.info(f"Data will be stored at: {provider_uri}")
        return True
    except Exception as e:
        logger.error(f"Failed to initialize qlib: {e}")
        return False


def get_future_instruments(contract_codes: List[str]) -> List[str]:
    """
    Get valid future instrument IDs for the specified contract codes.
    
    This function:
    1. Retrieves all available future instruments from Qlib
    2. Filters them to match the provided contract codes
    3. Returns the complete list of matching instrument IDs
    
    For example, if 'CU' is provided, it will return all copper future contracts
    like 'CU2301', 'CU2302', etc.
    
    Args:
        contract_codes: List of contract codes to filter by (e.g., ['CU', 'AL'])
        
    Returns:
        List[str]: List of valid instrument IDs that match the contract codes
        
    Raises:
        Any exceptions during instrument retrieval will be caught and logged
    """
    try:
        # Get all future instruments
        all_instruments = Instrument.get_instruments(inst_type='future')
        
        # Filter instruments by contract codes
        filtered_instruments = []
        for code in contract_codes:
            # Match instruments that start with the contract code followed by digits
            matching = [inst for inst in all_instruments if inst.startswith(code.upper()) and inst[len(code):].isdigit()]
            filtered_instruments.extend(matching)
        
        logger.info(f"Found {len(filtered_instruments)} instruments for contracts: {', '.join(contract_codes)}")
        return filtered_instruments
    except Exception as e:
        logger.error(f"Error getting future instruments: {e}")
        return []


def load_existing_data(output_path: str, filename: str) -> Optional[pd.DataFrame]:
    """
    Load existing data file if it exists, for resuming download.
    
    This function is used for the resume functionality. It checks if a CSV data file
    already exists and attempts to load it if available.
    
    Args:
        output_path: Path to the directory where data files are stored
        filename: Base filename without extension (e.g., 'future_data')
        
    Returns:
        Optional[pd.DataFrame]: Loaded data DataFrame if the file exists and can be loaded,
                               None otherwise
        
    Notes:
        - The function assumes the data uses a MultiIndex with instrument and date
        - Any errors during loading are caught and logged
    """
    csv_path = Path(output_path) / f"{filename}.csv"
    if csv_path.exists():
        try:
            logger.info(f"Found existing data file: {csv_path}")
            data = pd.read_csv(csv_path, index_col=[0, 1])  # MultiIndex for instrument and date
            logger.info(f"Loaded {len(data)} rows from existing file")
            return data
        except Exception as e:
            logger.error(f"Error loading existing data: {e}")
    return None


def get_latest_date(data: pd.DataFrame) -> Optional[str]:
    """
    Extract the latest date from the existing data DataFrame.
    
    This function is used for the resume functionality to determine where to start
    downloading new data from.
    
    Args:
        data: Existing data DataFrame with a MultiIndex (instrument, date)
        
    Returns:
        Optional[str]: Latest date in YYYY-MM-DD format if data exists, None otherwise
        
    Notes:
        - Assumes the DataFrame has a MultiIndex where level 1 contains dates
        - Dates are converted to datetime objects for proper comparison
    """
    if data is None or data.empty:
        return None
    
    # Get the latest date from the index (assuming DatetimeIndex level 1)
    if isinstance(data.index, pd.MultiIndex) and len(data.index.levels) >= 2:
        # Convert to datetime and find max
        latest_dt = pd.to_datetime(data.index.get_level_values(1)).max()
        return latest_dt.strftime('%Y-%m-%d')
    return None


def download_futures_data_in_chunks(
    instruments: List[str],
    freq: str,
    fields: List[str],
    output_path: str,
    filename: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    chunk_size: int = 30,
    resume: bool = False
) -> Optional[pd.DataFrame]:
    """
    Download futures data in manageable chunks with built-in resume capability.
    
    This is the core function for downloading data with robustness features. It:
    1. Handles resuming from previous downloads if requested
    2. Divides the date range into smaller chunks for efficient processing
    3. Saves checkpoints after each chunk to enable resumption
    4. Merges chunk data while avoiding duplicates
    5. Handles errors gracefully by saving partial progress
    
    Args:
        instruments: List of instrument IDs to download data for
        freq: Data frequency (e.g., '1min', '5min', '1d')
        fields: List of data fields to download (e.g., ['open', 'close', 'volume'])
        output_path: Path where checkpoint and final data will be saved
        filename: Base filename for the data files (without extension)
        start_date: Start date for the data download (YYYY-MM-DD format)
        end_date: End date for the data download (YYYY-MM-DD format)
        chunk_size: Number of days to download in each chunk (default: 30)
        resume: Whether to attempt resuming from an existing data file
        
    Returns:
        Optional[pd.DataFrame]: The complete downloaded data if successful,
                               or None if the download failed
        
    Notes:
        - Field names are automatically prefixed with '$' for Qlib compatibility
        - Default date range is the last 365 days if not specified
        - Checkpoint files are removed after successful completion
    """
    try:
        # Convert fields to qlib format (add $ prefix)
        qlib_fields = [f"${field}" for field in fields]
        
        # Check if we need to resume
        existing_data = None
        if resume:
            existing_data = load_existing_data(output_path, filename)
            latest_date = get_latest_date(existing_data)
            if latest_date:
                logger.info(f"Resuming download from {latest_date}")
                # Start from the day after the latest date
                start_date = (pd.to_datetime(latest_date) + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
                # If start date is beyond end date, we're done
                if end_date and pd.to_datetime(start_date) > pd.to_datetime(end_date):
                    logger.info("All data already downloaded, nothing to resume")
                    return existing_data
        
        # If no date range specified, use current date as end date
        if not end_date:
            end_date = pd.Timestamp.now().strftime('%Y-%m-%d')
        
        # If no start date specified, use a reasonable default
        if not start_date:
            start_date = (pd.to_datetime(end_date) - pd.Timedelta(days=365)).strftime('%Y-%m-%d')
        
        # Convert to datetime objects for range processing
        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date)
        
        # Calculate total days
        total_days = (end_dt - start_dt).days
        if total_days <= 0:
            logger.error("Invalid date range: start date must be before end date")
            return existing_data if existing_data is not None else None
        
        logger.info(f"Total download period: {total_days} days")
        
        # Initialize result with existing data if any
        result = existing_data.copy() if existing_data is not None else None
        
        # Process in chunks
        current_start = start_dt
        while current_start <= end_dt:
            # Calculate chunk end date
            chunk_end = min(current_start + pd.Timedelta(days=chunk_size - 1), end_dt)
            
            # Convert to string format
            chunk_start_str = current_start.strftime('%Y-%m-%d')
            chunk_end_str = chunk_end.strftime('%Y-%m-%d')
            
            logger.info(f"Downloading chunk: {chunk_start_str} to {chunk_end_str}")
            
            try:
                # Download chunk data
                chunk_data = D.features(
                    instruments=instruments,
                    fields=qlib_fields,
                    freq=freq,
                    start_time=chunk_start_str,
                    end_time=chunk_end_str
                )
                
                if chunk_data is None or chunk_data.empty:
                    logger.warning(f"No data found for chunk: {chunk_start_str} to {chunk_end_str}")
                else:
                    logger.info(f"Successfully downloaded chunk with {len(chunk_data)} rows")
                    
                    # Append to result
                    if result is None:
                        result = chunk_data
                    else:
                        # Avoid duplicates
                        if not chunk_data.index.isin(result.index).all():
                            result = pd.concat([result, chunk_data[~chunk_data.index.isin(result.index)]])
                        else:
                            logger.info("No new data in this chunk")
                    
                    # Save checkpoint after each chunk
                    temp_filename = f"{filename}_checkpoint"
                    temp_result = result.copy()
                    save_checkpoint(temp_result, output_path, temp_filename)
                    
                    # If this is the last chunk, save final result
                    if chunk_end >= end_dt:
                        save_checkpoint(result, output_path, filename)
            
            except Exception as e:
                logger.error(f"Error downloading chunk {chunk_start_str} to {chunk_end_str}: {e}")
                logger.warning("Saving progress so far...")
                if result is not None:
                    save_checkpoint(result, output_path, f"{filename}_partial")
                logger.error("Download failed. You can resume later using --resume flag")
                return result
            
            # Move to next chunk
            current_start = chunk_end + pd.Timedelta(days=1)
        
        logger.info(f"Completed download for all chunks")
        logger.info(f"Final data shape: {result.shape} if result is not None else 'No data'")
        
        # Remove checkpoint file after successful completion
        checkpoint_path = Path(output_path) / f"{filename}_checkpoint.csv"
        if checkpoint_path.exists():
            checkpoint_path.unlink()
            logger.info(f"Removed checkpoint file: {checkpoint_path}")
        
        return result
    except Exception as e:
        logger.error(f"Error in chunked download: {e}")
        return None


def save_checkpoint(data: pd.DataFrame, output_path: str, filename: str) -> bool:
    """
    Save data as a checkpoint during the download process.
    
    This function is used to save intermediate progress during chunked downloads,
    enabling the resume functionality. It saves data in both CSV and Parquet formats
    (when pyarrow is available).
    
    Args:
        data: DataFrame containing the data to save
        output_path: Path to the directory where checkpoints will be stored
        filename: Base filename for the checkpoint files (without extension)
        
    Returns:
        bool: True if the checkpoint was saved successfully, False otherwise
        
    Notes:
        - Automatically creates the output directory if it doesn't exist
        - Always saves in CSV format for compatibility
        - Saves in Parquet format if pyarrow is installed (better compression and performance)
        - Logs detailed information about the checkpoint saving process
    """
    try:
        # Ensure output directory exists
        output_dir = Path(output_path)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Save as CSV for checkpoint
        csv_path = output_dir / f"{filename}.csv"
        data.to_csv(csv_path)
        logger.info(f"Checkpoint saved to: {csv_path}")
        
        # Also save as parquet if possible
        try:
            import pyarrow
            parquet_path = output_dir / f"{filename}.parquet"
            data.to_parquet(parquet_path)
            logger.debug(f"Checkpoint saved to Parquet: {parquet_path}")
        except ImportError:
            pass
        
        return True
    except Exception as e:
        logger.error(f"Error saving checkpoint: {e}")
        return False


def download_futures_data(
    instruments: List[str],
    freq: str,
    fields: List[str],
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
) -> Optional[pd.DataFrame]:
    """
    Download futures data using Qlib's D.features API in a single request.
    
    This is a simplified version of the download function that retrieves data
    for the entire date range in one go. It's less robust than the chunked version
    but can be faster for smaller datasets.
    
    Args:
        instruments: List of instrument IDs to download data for
        freq: Data frequency (e.g., '1min', '5min', '1d')
        fields: List of data fields to download (e.g., ['open', 'close', 'volume'])
        start_date: Optional start date for the data download (YYYY-MM-DD format)
        end_date: Optional end date for the data download (YYYY-MM-DD format)
        
    Returns:
        Optional[pd.DataFrame]: The downloaded data if successful, None if failed
        
    Notes:
        - Field names are automatically prefixed with '$' for Qlib compatibility
        - Does not include resume functionality
        - Not recommended for very large date ranges due to potential timeouts
    """
    try:
        # Convert fields to qlib format (add $ prefix)
        qlib_fields = [f"${field}" for field in fields]
        
        # Download data using qlib's D module
        data = D.features(
            instruments=instruments,
            fields=qlib_fields,
            freq=freq,
            start_time=start_date,
            end_time=end_date
        )
        
        logger.info(f"Successfully downloaded data for {len(instruments)} instruments")
        logger.info(f"Data shape: {data.shape}")
        
        return data
    except Exception as e:
        logger.error(f"Error downloading futures data: {e}")
        return None


def save_data(data: pd.DataFrame, output_path: str, filename: str = 'futures_data') -> bool:
    """
    Save the downloaded data to CSV and Parquet files.
    
    This function ensures the output directory exists and saves the data in
    both CSV (for compatibility) and Parquet (for efficiency) formats.
    
    Args:
        data: DataFrame containing the downloaded futures data
        output_path: Path to the directory where data files will be saved
        filename: Base filename for the output files (without extension)
        
    Returns:
        bool: True if data was saved successfully in at least one format,
              False if both formats failed
        
    Notes:
        - Automatically creates the output directory if it doesn't exist
        - Always saves in CSV format for maximum compatibility
        - Saves in Parquet format if pyarrow is available (better for analytics)
        - Logs information about successful saves and warnings/errors
    """
    try:
        # Ensure output directory exists
        output_dir = Path(output_path)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Save as CSV
        csv_path = output_dir / f"{filename}.csv"
        data.to_csv(csv_path)
        logger.info(f"Data saved to CSV: {csv_path}")
        
        # Save as Parquet for better performance and compression
        try:
            import pyarrow
            parquet_path = output_dir / f"{filename}.parquet"
            data.to_parquet(parquet_path)
            logger.info(f"Data saved to Parquet: {parquet_path}")
        except ImportError:
            logger.warning("pyarrow not installed, skipping Parquet format")
        
        return True
    except Exception as e:
        logger.error(f"Error saving data: {e}")
        return False


def main():
    """
    Main function that orchestrates the futures data download process.
    
    This function implements the complete workflow:
    1. Parses command-line arguments
    2. Configures logging based on verbosity level
    3. Initializes the Qlib environment
    4. Processes contract codes and retrieves valid instruments
    5. Handles field specification and data range
    6. Downloads data using the chunked approach with resume capability
    7. Sorts the final data for consistency
    8. Saves the results to both CSV and Parquet files
    
    Exits with non-zero status code on any critical error.
    """
    # Parse command-line arguments
    parser = setup_argparse()
    args = parser.parse_args()
    
    # Set logging level based on verbose flag
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    
    # Initialize qlib
    if not initialize_qlib(args.output_path, args.region):
        sys.exit(1)
    
    # Parse contract codes
    contract_codes = [code.strip() for code in args.contracts.split(',')]
    logger.info(f"Downloading data for contracts: {', '.join(contract_codes)}")
    
    # Get valid future instruments
    instruments = get_future_instruments(contract_codes)
    if not instruments:
        logger.error("No valid instruments found for the specified contracts")
        sys.exit(1)
    
    # Parse fields
    fields = [field.strip() for field in args.fields.split(',')]
    
    # Download data
    logger.info(f"Downloading {args.freq} data for fields: {', '.join(fields)}")
    if args.start_date and args.end_date:
        logger.info(f"Date range: {args.start_date} to {args.end_date}")
    
    # Use chunked download with resume capability
    filename = f"futures_{'_'.join(contract_codes)}_{args.freq}"
    data = download_futures_data_in_chunks(
        instruments=instruments,
        freq=args.freq,
        fields=fields,
        output_path=args.output_path,
        filename=filename,
        start_date=args.start_date,
        end_date=args.end_date,
        chunk_size=args.chunk_size,
        resume=args.resume
    )
    
    if data is None:
        logger.error("Failed to download data")
        sys.exit(1)
    
    # Ensure the data is sorted by instrument and date
    if isinstance(data.index, pd.MultiIndex):
        data = data.sort_index()
    
    # Save final data (already saved during chunking, but ensure final version is saved)
    if not save_data(data, args.output_path, filename):
        logger.error("Failed to save final data")
        sys.exit(1)
    
    logger.info("Data download and save completed successfully!")


if __name__ == "__main__":
    main()