#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Simple VNPY Data Download Script for Rebar Futures

This script downloads historical daily data for rebar futures (螺纹钢) from SHFE exchange.
"""

import os
import sys
import logging
from datetime import datetime
import pandas as pd
from pathlib import Path

# Import VNPY modules
try:
    from vnpy.trader.constant import Exchange, Interval
    from vnpy.trader.datafeed import get_datafeed
    from vnpy.trader.object import HistoryRequest
except ImportError:
    print("vnpy module not found. Please install it with: pip install vnpy>=3.1.0")
    sys.exit(1)

# Configure basic logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("rebar_data_download")

def initialize_datafeed(datafeed_name: str = "REST") -> bool:
    """
    Initialize VNPY datafeed.
    """
    try:
        datafeed = get_datafeed()
        datafeed.init()
        logger.info(f"Datafeed  initialized successfully")
        return True
    except Exception as e:
        logger.error(f"Failed to initialize datafeed: {e}")
        return False

def download_rebar_data(start_date: datetime, end_date: datetime, output_dir: str = "./") -> None:
    """
    Download rebar futures daily data.
    """
    # Symbol and exchange information
    symbol = "RB"
    exchange = Exchange.SHFE
    interval_str = "d"
    interval = Interval.DAILY
    
    logger.info(f"Starting to download {symbol}@{exchange} daily data from {start_date} to {end_date}")
    
    # Create history request
    req = HistoryRequest(
        symbol=symbol,
        exchange=exchange,
        interval=interval,
        start=start_date,
        end=end_date
    )
    
    try:
        # Get datafeed and download data
        datafeed = get_datafeed()
        bars = datafeed.query_history(req)
        
        if not bars:
            logger.warning("No data returned for the specified period")
            return
        
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
        
        logger.info(f"Downloaded {len(df)} bars of data")
        
        # Ensure output directory exists
        output_dir_path = Path(output_dir)
        output_dir_path.mkdir(parents=True, exist_ok=True)
        
        # Generate filename and save to CSV
        filename = f"{symbol}_{exchange.value}_{interval_str}_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.csv"
        file_path = output_dir_path / filename
        df.to_csv(file_path)
        
        logger.info(f"Data saved to: {file_path}")
        
    except Exception as e:
        logger.error(f"Error during data download or saving: {e}")

def main():
    """
    Main function to download rebar futures data.
    """
    # Set date range
    start_date = datetime(2013, 1, 1)
    end_date = datetime(2025, 11, 13)
    
    # Initialize datafeed
    if not initialize_datafeed():
        logger.error("Failed to initialize datafeed, exiting...")
        sys.exit(1)
    
    # Download data
    download_rebar_data(start_date, end_date, output_dir="./rebar_data")
    
    logger.info("Data download process completed")

if __name__ == "__main__":
    main()