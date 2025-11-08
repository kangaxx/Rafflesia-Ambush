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
        print("\nExamples will be skipped.")
        return
    
    try:
        # Run examples
        example_basic_usage()
        example_custom_date_range()
        example_raw_data_only()
        example_custom_features()
        example_save_data()
        
    except ImportError as e:
        print(f"\n✗ Import error: {e}")
        print("Make sure qlib is installed: pip install pyqlib")
    except Exception as e:
        print(f"\n✗ Error running examples: {e}")
    
    print("\n" + "=" * 60)
    print("Examples completed!")
    print("=" * 60)


if __name__ == "__main__":
    main()
