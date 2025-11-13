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
        description="Download futures data using qlib with customizable parameters"
    )
    
    # Data storage configuration
    parser.add_argument(
        '--output-path',
        type=str,
        default='~/.qlib/qlib_data/cn_future',
        help='Path to save the downloaded futures data (default: ~/.qlib/qlib_data/cn_future)'
    )
    
    # Contract specifications
    parser.add_argument(
        '--contracts',
        type=str,
        required=True,
        help='Future contracts to download, separated by commas (e.g., "CU,AL,ZN")'
    )
    
    # Data frequency
    parser.add_argument(
        '--freq',
        type=str,
        default='1d',
        choices=['1min', '5min', '15min', '30min', '60min', '1d'],
        help='Data frequency (default: 1d)'
    )
    
    # Date range
    parser.add_argument(
        '--start-date',
        type=str,
        help='Start date in YYYY-MM-DD format'
    )
    
    parser.add_argument(
        '--end-date',
        type=str,
        help='End date in YYYY-MM-DD format'
    )
    
    # Data fields
    parser.add_argument(
        '--fields',
        type=str,
        default='open,high,low,close,volume,amount',
        help='Data fields to download, separated by commas (default: open,high,low,close,volume,amount)'
    )
    
    # Region
    parser.add_argument(
        '--region',
        type=str,
        default='cn',
        choices=['cn', 'us'],
        help='Data region (default: cn)'
    )
    
    # Logging level
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )
    
    # Resume download
    parser.add_argument(
        '--resume',
        action='store_true',
        help='Resume download from previous checkpoint if available'
    )
    
    # Download chunk size (days)
    parser.add_argument(
        '--chunk-size',
        type=int,
        default=30,
        help='Number of days to download in each chunk (default: 30)'
    )
    
    return parser


def initialize_qlib(provider_uri: str, region: str) -> bool:
    """
    Initialize qlib with the specified configuration.
    
    Args:
        provider_uri: Path to store qlib data
        region: Data region (cn/us)
        
    Returns:
        bool: True if initialization successful, False otherwise
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
    
    Args:
        contract_codes: List of contract codes (e.g., ['CU', 'AL'])
        
    Returns:
        List[str]: List of valid instrument IDs
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
    
    Args:
        output_path: Path to the data directory
        filename: Base filename without extension
        
    Returns:
        Optional[pd.DataFrame]: Loaded data if exists, None otherwise
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
    Get the latest date from the existing data.
    
    Args:
        data: Existing data DataFrame
        
    Returns:
        Optional[str]: Latest date in YYYY-MM-DD format, or None if no data
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
    Download futures data in chunks with resume capability.
    
    Args:
        instruments: List of instrument IDs
        freq: Data frequency
        fields: List of data fields to download
        output_path: Path to save checkpoint data
        filename: Base filename for checkpoint
        start_date: Start date
        end_date: End date
        chunk_size: Number of days to download in each chunk
        resume: Whether to resume from previous checkpoint
        
    Returns:
        Optional[pd.DataFrame]: Downloaded data, or None if failed
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
    Save data as a checkpoint.
    
    Args:
        data: DataFrame to save
        output_path: Path to save directory
        filename: Base filename without extension
        
    Returns:
        bool: True if successful, False otherwise
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
    Download futures data using qlib.
    
    Args:
        instruments: List of instrument IDs
        freq: Data frequency
        fields: List of data fields to download
        start_date: Start date
        end_date: End date
        
    Returns:
        Optional[pd.DataFrame]: Downloaded data, or None if failed
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
    Save the downloaded data to files.
    
    Args:
        data: DataFrame containing the downloaded data
        output_path: Path to save the data
        filename: Base filename without extension
        
    Returns:
        bool: True if successful, False otherwise
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
    Main function to run the futures data downloader.
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