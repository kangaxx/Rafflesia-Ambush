#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
VNPY Data Download Tool

This script allows users to download historical market data using VNPY framework.
It supports various exchanges, symbols, time intervals, and date ranges.

Examples:
    Download daily data for BTCUSDT from Binance:
    $ python vnpy_data_download.py --symbol BTCUSDT --interval d --start 2023-01-01 --end 2023-12-31 --exchange BINANCE --output ./data

    Download 1-hour data for multiple symbols:
    $ python vnpy_data_download.py --symbol BTCUSDT,ETHUSDT --interval h1 --start 2023-01-01 --end 2023-01-31
"""

import os
import sys
import logging
import argparse
from datetime import datetime
from typing import List, Optional

import pandas as pd
from pathlib import Path

# Import VNPY modules
try:
    from vnpy.trader.constant import Exchange, Interval
    from vnpy.trader.datafeed import get_datafeed
    from vnpy.trader.object import HistoryRequest
except ImportError:
    logging.error("vnpy module not found. Please install it with: pip install vnpy>=3.1.0")
    sys.exit(1)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("vnpy_data_download.log")
    ]
)
logger = logging.getLogger("vnpy_data_download")

def setup_argparse() -> argparse.ArgumentParser:
    """
    Set up command-line argument parser for VNPY data download tool.
    
    Returns:
        argparse.ArgumentParser: Configured argument parser
    """
    parser = argparse.ArgumentParser(
        description="VNPY Historical Data Download Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Example usage:\n"
               "  python vnpy_data_download.py --symbol BTCUSDT --interval d --start 2023-01-01 --end 2023-12-31\n"
               "  python vnpy_data_download.py  --symbol ag,rb --interval d --exchange SHFE --start 2013-01-01 --end 2025-11-13\n"
               "  python vnpy_data_download.py --symbol BTCUSDT,ETHUSDT --interval h1 --output ./downloads\n",
        add_help=False
    )
    
    # Add help parameter explicitly to support both -h and --help
    parser.add_argument(
        "-h",
        "--help",
        action="help",
        help="Show this help message and exit"
    )
    
    # Core parameters
    parser.add_argument(
        "--symbol",
        type=str,
        required=True,
        help="Trading symbol(s) to download data for (comma-separated if multiple), e.g., 'BTCUSDT,ETHUSDT'"
    )
    
    parser.add_argument(
        "--interval",
        type=str,
        required=True,
        help="Time interval for data. The input will be passed directly to VNPY with basic conversion."
        " Common formats: '1m', '5m', '15m', 'h1', 'd', etc."
    )
    
    parser.add_argument(
        "--start",
        type=str,
        required=True,
        help="Start date for data download (YYYY-MM-DD format), e.g., '2023-01-01'"
    )
    
    parser.add_argument(
        "--end",
        type=str,
        required=True,
        help="End date for data download (YYYY-MM-DD format), e.g., '2023-12-31'"
    )
    
    # Optional parameters
    parser.add_argument(
        "--exchange",
        type=str,
        default="BINANCE",
        help="Exchange name, default is 'BINANCE'. Supported exchanges depend on VNPY configuration."
    )
    
    parser.add_argument(
        "--datafeed",
        type=str,
        default="REST",
        help="Data feed type, default is 'REST'. Some exchanges may require specific data feeds."
    )
    
    parser.add_argument(
        "--output",
        type=str,
        default=".",
        help="Output directory for downloaded data files, default is current directory."
    )
    
    parser.add_argument(
        "--format",
        type=str,
        choices=["csv", "parquet"],
        default="csv",
        help="Output file format, default is 'csv'. 'parquet' offers better compression but requires pyarrow."
    )
    
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging mode for more detailed output."
    )
    
    return parser

def parse_interval(interval_str: str) -> Interval:
    """
    Convert interval string to VNPY Interval enum.
    
    Args:
        interval_str: Interval string as input by user
        
    Returns:
        Interval: Corresponding VNPY Interval enum value
        
    Raises:
        ValueError: If the interval string cannot be mapped to a supported Interval
    """
    # Keep simple mapping for common intervals, but allow any input
    interval_map = {
        "1m": Interval.MINUTE,
        "5m": Interval.MINUTE_5,
        "15m": Interval.MINUTE_15,
        "30m": Interval.MINUTE_30,
        "60m": Interval.MINUTE_60,
        "h1": Interval.HOUR,
        "h4": Interval.HOUR_4,
        "d": Interval.DAILY,
        "w": Interval.WEEKLY,
        "M": Interval.MONTHLY
    }
    
    # Try exact match first
    if interval_str in interval_map:
        return interval_map[interval_str]
    
    # Try case-insensitive match
    interval_str_lower = interval_str.lower()
    for key, value in interval_map.items():
        if key.lower() == interval_str_lower:
            return value
    
    # If no match found, try to infer based on common patterns
    try:
        if interval_str_lower.startswith("m"):
            # Handle minute intervals like 'm1', 'm5'
            minutes = int(interval_str_lower[1:])
            if minutes == 1:
                return Interval.MINUTE
            elif minutes == 5:
                return Interval.MINUTE_5
            elif minutes == 15:
                return Interval.MINUTE_15
            elif minutes == 30:
                return Interval.MINUTE_30
            elif minutes == 60:
                return Interval.MINUTE_60
        elif interval_str_lower.startswith("h"):
            # Handle hour intervals like 'h1', 'h4'
            hours = int(interval_str_lower[1:])
            if hours == 1:
                return Interval.HOUR
            elif hours == 4:
                return Interval.HOUR_4
        elif interval_str_lower == "day" or interval_str_lower == "daily":
            return Interval.DAILY
        elif interval_str_lower == "week" or interval_str_lower == "weekly":
            return Interval.WEEKLY
        elif interval_str_lower == "month" or interval_str_lower == "monthly":
            return Interval.MONTHLY
    except ValueError:
        pass
    
    # If we can't map it, raise an error
    raise ValueError(f"Could not map interval '{interval_str}' to a supported VNPY interval.\n" 
                     f"Commonly used intervals: {list(interval_map.keys())}")

def parse_exchange(exchange_str: str) -> Exchange:
    """
    Convert exchange string to VNPY Exchange enum.
    
    Args:
        exchange_str: Exchange string (e.g., 'BINANCE', 'OKEX')
        
    Returns:
        Exchange: Corresponding VNPY Exchange enum value
        
    Raises:
        ValueError: If the exchange string is not supported
    """
    try:
        return Exchange[exchange_str.upper()]
    except KeyError:
        raise ValueError(f"Unsupported exchange: {exchange_str}. Please check VNPY documentation for supported exchanges.")

def initialize_datafeed(datafeed_name: str) -> bool:
    """
    Initialize the VNPY datafeed.
    
    Args:
        datafeed_name: Name of the datafeed to use
        
    Returns:
        bool: True if initialization successful, False otherwise
    """
    try:
        datafeed = get_datafeed(datafeed_name)
        datafeed.init()
        logger.info(f"Datafeed '{datafeed_name}' initialized successfully")
        return True
    except Exception as e:
        logger.error(f"Failed to initialize datafeed '{datafeed_name}': {e}")
        return False

def download_symbol_data(
    symbol: str,
    exchange: Exchange,
    interval: Interval,
    start_date: datetime,
    end_date: datetime
) -> Optional[pd.DataFrame]:
    """
    Download historical data for a single symbol.
    
    Args:
        symbol: Trading symbol
        exchange: Exchange enum
        interval: Time interval
        start_date: Start datetime
        end_date: End datetime
        
    Returns:
        Optional[pd.DataFrame]: Downloaded data as pandas DataFrame, or None if failed
    """
    try:
        # Create history request
        req = HistoryRequest(
            symbol=symbol,
            exchange=exchange,
            interval=interval,
            start=start_date,
            end=end_date
        )
        
        # Get datafeed and download data
        datafeed = get_datafeed()
        bars = datafeed.query_history(req)
        
        if not bars:
            logger.warning(f"No data returned for {symbol}@{exchange}")
            return None
        
        # Convert bars to pandas DataFrame
        data = []
        for bar in bars:
            data.append({
                "datetime": bar.datetime,
                "open": bar.open_price,
                "high": bar.high_price,
                "low": bar.low_price,
                "close": bar.close_price,
                "volume": bar.volume,
                "turnover": bar.turnover if hasattr(bar, 'turnover') else 0
            })
        
        df = pd.DataFrame(data)
        df.set_index("datetime", inplace=True)
        
        logger.info(f"Downloaded {len(df)} bars for {symbol}@{exchange}, interval: {interval}")
        return df
        
    except Exception as e:
        logger.error(f"Error downloading data for {symbol}@{exchange}: {e}")
        return None

def save_data(df: pd.DataFrame, output_path: str, filename: str, format_type: str = "csv") -> bool:
    """
    Save downloaded data to file.
    
    Args:
        df: DataFrame containing the data
        output_path: Directory path to save the file
        filename: Base filename without extension
        format_type: Output format ('csv' or 'parquet')
        
    Returns:
        bool: True if data was saved successfully, False otherwise
    """
    try:
        # Ensure output directory exists
        output_dir = Path(output_path)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        if format_type.lower() == "csv":
            file_path = output_dir / f"{filename}.csv"
            df.to_csv(file_path)
            logger.info(f"Data saved to CSV: {file_path}")
        elif format_type.lower() == "parquet":
            try:
                file_path = output_dir / f"{filename}.parquet"
                df.to_parquet(file_path)
                logger.info(f"Data saved to Parquet: {file_path}")
            except ImportError:
                logger.warning("pyarrow not installed, falling back to CSV format")
                file_path = output_dir / f"{filename}.csv"
                df.to_csv(file_path)
                logger.info(f"Data saved to CSV: {file_path}")
        else:
            logger.error(f"Unsupported format: {format_type}")
            return False
        
        return True
    except Exception as e:
        logger.error(f"Error saving data: {e}")
        return False

def main():
    """
    Main function to run the VNPY data download tool.
    """
    # Parse command-line arguments
    parser = setup_argparse()
    args = parser.parse_args()
    
    # Set logging level based on verbose flag
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    
    try:
        # Parse symbols
        symbols = [s.strip() for s in args.symbol.split(",")]
        logger.info(f"Downloading data for symbols: {symbols}")
        
        # Parse interval
        interval = parse_interval(args.interval)
        logger.info(f"Time interval (user input: {args.interval}, mapped to: {interval})")
        
        # Parse exchange
        exchange = parse_exchange(args.exchange)
        logger.info(f"Exchange: {exchange}")
        
        # Parse dates
        start_date = datetime.strptime(args.start, "%Y-%m-%d")
        end_date = datetime.strptime(args.end, "%Y-%m-%d")
        
        # Set end_date to end of day
        end_date = end_date.replace(hour=23, minute=59, second=59)
        
        logger.info(f"Date range: {start_date} to {end_date}")
        
        # Initialize datafeed
        if not initialize_datafeed(args.datafeed):
            logger.error("Failed to initialize datafeed, exiting...")
            sys.exit(1)
        
        # Download data for each symbol
        for symbol in symbols:
            logger.info(f"Processing symbol: {symbol}")
            
            # Download data
            df = download_symbol_data(symbol, exchange, interval, start_date, end_date)
            
            if df is not None and not df.empty:
                # Generate filename
                interval_str = args.interval
                filename = f"{symbol}_{exchange.value}_{interval_str}_{args.start}_{args.end}"
                
                # Save data
                if save_data(df, args.output, filename, args.format):
                    logger.info(f"Successfully downloaded and saved data for {symbol}")
                else:
                    logger.error(f"Failed to save data for {symbol}")
            else:
                logger.warning(f"No data available for {symbol}")
        
        logger.info("Data download process completed")
        
    except ValueError as e:
        logger.error(f"Value error: {e}")
        parser.print_help()
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()