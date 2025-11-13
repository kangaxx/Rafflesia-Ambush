#!/usr/bin/env python
"""
Example script demonstrating how to use the qlib training data processor.

This script shows various usage patterns and best practices.
"""

import sys
import os
from pathlib import Path

# Add src directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from data_processor import QlibDataProcessor


def example_basic_usage():
    """
    Example 1: Basic usage - fetch and process data with default settings.
    """
    print("\n=== Example 1: Basic Usage ===")
    
    # Initialize processor
    processor = QlibDataProcessor(
        provider_uri='~/.qlib/qlib_data/cn_data',
        region='cn'
    )
    
    # Initialize qlib
    if not processor.initialize():
        print("Failed to initialize qlib. Make sure data is downloaded.")
        return
    
    # Prepare training data
    features, labels = processor.prepare_training_data(
        instruments='csi300',
        start_time='2023-01-01',
        end_time='2023-12-31',
        generate_features=True
    )
    
    if features is not None:
        print(f"✓ Generated features with shape: {features.shape}")
        print(f"✓ Columns: {list(features.columns)[:10]}...")  # Show first 10
        
        if labels is not None:
            print(f"✓ Generated labels with shape: {labels.shape}")
    else:
        print("✗ Failed to generate training data")


def example_custom_date_range():
    """
    Example 2: Fetch data for a custom date range.
    """
    print("\n=== Example 2: Custom Date Range ===")
    
    processor = QlibDataProcessor(region='cn')
    
    if not processor.initialize():
        print("Failed to initialize qlib")
        return
    
    # Fetch data for Q1 2023
    features, labels = processor.prepare_training_data(
        instruments='csi300',
        start_time='2023-01-01',
        end_time='2023-03-31',
        generate_features=True
    )
    
    if features is not None:
        print(f"✓ Q1 2023 data: {features.shape}")


def example_save_data():
    """
    Example 3: Fetch data and save to different formats.
    """
    print("\n=== Example 3: Save Data in Different Formats ===")
    
    processor = QlibDataProcessor(region='cn')
    
    if not processor.initialize():
        print("Failed to initialize qlib")
        return
    
    # Prepare data
    features, labels = processor.prepare_training_data(
        instruments='csi300',
        start_time='2023-01-01',
        end_time='2023-01-31',  # Small range for quick example
        generate_features=True
    )
    
    if features is None:
        print("✗ No data to save")
        return


def example_futures_data_download():
    """
    Example 4: Download futures data using the qlib_download_future_data script.
    
    This example demonstrates how to use the futures data downloader script
    with different command-line parameters.
    """
    print("\n=== Example 4: Futures Data Download ===")
    print("\nThe following are example commands for downloading futures data:")
    print("\n1. Basic usage - download multiple contracts:")
    print("   python src/qlib_download_future_data.py --contracts CU,AL,ZN")
    
    print("\n2. Specify custom output path:")
    print("   python src/qlib_download_future_data.py --contracts RU,BU --output-path ./data/futures")
    
    print("\n3. Set data frequency (e.g., 5-minute data):")
    print("   python src/qlib_download_future_data.py --contracts IF,IC --freq 5min")
    
    print("\n4. Download data for a specific date range:")
    print("   python src/qlib_download_future_data.py --contracts SC,MA --start-date 2023-01-01 --end-date 2023-12-31")
    
    print("\n5. Customize data fields:")
    print("   python src/qlib_download_future_data.py --contracts FG,ZC --fields open,close,volume")
    
    print("\n6. Enable verbose logging:")
    print("   python src/qlib_download_future_data.py --contracts A,B --verbose")
    
    print("\n7. Use resume functionality (after interruption):")
    print("   python src/qlib_download_future_data.py --contracts CU,AL --resume")
    
    print("\n8. Adjust chunk size for large date ranges:")
    print("   python src/qlib_download_future_data.py --contracts RU,BU --chunk-size 15 --start-date 2020-01-01 --end-date 2023-12-31")
    
    print("\n9. Resume with custom chunk size:")
    print("   python src/qlib_download_future_data.py --contracts CU,AL,ZN --resume --chunk-size 10")


def example_futures_data_integration():
    """
    Example 5: Integrate downloaded futures data with the data processor.
    
    This example shows how to use the downloaded futures data with the QlibDataProcessor
    for further processing or feature engineering.
    """
    print("\n=== Example 5: Futures Data Integration ===")
    
    # Note: This example assumes you have already downloaded futures data using
    # the qlib_download_future_data.py script
    
    # The steps would be:
    # 1. Download futures data first using the command-line script
    # 2. Load the downloaded data
    # 3. Process it using QlibDataProcessor or other tools
    
    print("Integration workflow:")
    print("1. Download data: python src/qlib_download_future_data.py --contracts CU,AL --output-path ./futures_data")
    print("2. Load the data using pandas:")
    print("   import pandas as pd")
    print("   futures_data = pd.read_csv('./futures_data/futures_CU_AL_1d.csv')")
    print("3. Process the data as needed for your trading strategy")


def run_all_examples():
    """Run all example functions."""
    example_basic_usage()
    example_custom_date_range()
    example_save_data()
    example_futures_data_download()
    example_futures_data_integration()


if __name__ == "__main__":
    run_all_examples()
    
    # Create output directory
    output_dir = Path('output')
    output_dir.mkdir(exist_ok=True)
    
    # Save in different formats
    formats = ['csv', 'parquet', 'pickle']
    for fmt in formats:
        output_path = output_dir / f'training_data.{fmt}'
        if processor.save_data(features, str(output_path), format=fmt):
            print(f"✓ Saved data as {fmt}: {output_path}")
        else:
            print(f"✗ Failed to save as {fmt}")


def example_raw_data_only():
    """
    Example 4: Fetch only raw data without feature generation.
    """
    print("\n=== Example 4: Raw Data Only ===")
    
    processor = QlibDataProcessor(region='cn')
    
    if not processor.initialize():
        print("Failed to initialize qlib")
        return
    
    # Fetch raw data only
    data = processor.fetch_data(
        instruments='csi300',
        start_time='2023-01-01',
        end_time='2023-01-31'
    )
    
    if data is not None:
        print(f"✓ Fetched raw data: {data.shape}")
        print(f"✓ Columns: {list(data.columns)}")
    else:
        print("✗ Failed to fetch data")


def example_custom_features():
    """
    Example 5: Fetch data and generate custom features.
    """
    print("\n=== Example 5: Custom Features ===")
    
    processor = QlibDataProcessor(region='cn')
    
    if not processor.initialize():
        print("Failed to initialize qlib")
        return
    
    # Fetch raw data
    data = processor.fetch_data(
        instruments='csi300',
        start_time='2023-01-01',
        end_time='2023-01-31'
    )
    
    if data is None:
        print("✗ Failed to fetch data")
        return
    
    # Generate features
    features = processor.generate_features(data)
    
    if features is not None:
        print(f"✓ Original data: {data.shape}")
        print(f"✓ With features: {features.shape}")
        print(f"✓ New columns: {[col for col in features.columns if col not in data.columns]}")
    else:
        print("✗ Failed to generate features")


def example_vnpy_data_download():
    """
    Example 6: Using VNPY data download tool.
    
    This example demonstrates how to use the vnpy_data_download.py script
    to download historical market data from various exchanges.
    """
    print("\n=== Example 6: VNPY Data Download ===")
    print("This example shows command-line usage of vnpy_data_download.py")
    print("\n1. Download daily BTC data from Binance:")
    print("   python src/vnpy_data_download.py --symbol BTCUSDT --interval d --start 2023-01-01 --end 2023-01-10 --exchange BINANCE")
    print("\n2. Download hourly data for multiple symbols:")
    print("   python src/vnpy_data_download.py --symbol BTCUSDT,ETHUSDT --interval h1 --start 2023-01-01 --end 2023-01-02 --output ./downloads")
    print("\n3. Download minute data and save as Parquet:")
    print("   python src/vnpy_data_download.py --symbol BTCUSDT --interval 1m --start 2023-01-01 --end 2023-01-01 --format parquet")
    print("\n4. Download Shanghai Silver (沪银) futures data:")
    print("   python src/vnpy_data_download.py --symbol AG --interval d --start 2023-01-01 --end 2023-12-31 --exchange SHFE --output ./shfe_data")
    print("   # For specific contract month:")
    print("   python src/vnpy_data_download.py --symbol AG2312 --interval d --start 2023-01-01 --end 2023-12-31 --exchange SHFE")
    print("\nNote: Before running these commands, you need to install vnpy:")
    print("      pip install vnpy>=3.1.0")
    print("\nMake sure you have proper API access configured for the exchanges you want to use.")
    print("For Chinese futures markets (SHFE), you may need specific datafeed configuration.")


def main():
    """
    Run all examples.
    """
    print("=" * 60)
    print("Qlib Training Data Processor - Usage Examples")
    print("=" * 60)
    
    # Check if qlib data is available
    data_path = os.path.expanduser('~/.qlib/qlib_data/cn_data')
    if not os.path.exists(data_path):
        print("\n⚠ WARNING: Qlib data not found at", data_path)
        print("Please download data first:")
        print("python -m qlib.run.get_data qlib_data --target_dir ~/.qlib/qlib_data/cn_data --region cn")
        print("\nQLib examples will be skipped.")
    
    try:
        # Run Qlib examples if data is available
        if os.path.exists(data_path):
            example_basic_usage()
            example_custom_date_range()
            example_raw_data_only()
            example_custom_features()
            example_save_data()
        
        # Always show VNPY example (doesn't require Qlib data)
        example_vnpy_data_download()
        
    except ImportError as e:
        print(f"\n✗ Import error: {e}")
        print("Make sure required packages are installed.")
    except Exception as e:
        print(f"\n✗ Error running examples: {e}")
    
    print("\n" + "=" * 60)
    print("Examples completed!")
    print("=" * 60)


if __name__ == "__main__":
    main()
