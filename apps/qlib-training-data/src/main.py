"""
Main entry point for qlib training data generation.

This script provides a command-line interface for generating training data
using qlib for AI trading strategy development.
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import Optional

from data_processor import QlibDataProcessor


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def setup_argparse() -> argparse.ArgumentParser:
    """
    Set up command-line argument parser.
    
    Returns:
        argparse.ArgumentParser: Configured argument parser
    """
    parser = argparse.ArgumentParser(
        description="Generate training data using qlib for AI trading strategies"
    )
    
    parser.add_argument(
        '--provider-uri',
        type=str,
        default='~/.qlib/qlib_data/cn_data',
        help='Path to qlib data provider (default: ~/.qlib/qlib_data/cn_data)'
    )
    
    parser.add_argument(
        '--region',
        type=str,
        default='cn',
        choices=['cn', 'us'],
        help='Data region (default: cn)'
    )
    
    parser.add_argument(
        '--instruments',
        type=str,
        default='csi300',
        help='Stock instruments to fetch (default: csi300)'
    )
    
    parser.add_argument(
        '--start-time',
        type=str,
        help='Start date in YYYY-MM-DD format'
    )
    
    parser.add_argument(
        '--end-time',
        type=str,
        help='End date in YYYY-MM-DD format'
    )
    
    parser.add_argument(
        '--output',
        type=str,
        default='training_data.csv',
        help='Output file path (default: training_data.csv)'
    )
    
    parser.add_argument(
        '--format',
        type=str,
        default='csv',
        choices=['csv', 'parquet', 'pickle'],
        help='Output file format (default: csv)'
    )
    
    parser.add_argument(
        '--no-features',
        action='store_true',
        help='Skip feature generation (only fetch raw data)'
    )
    
    parser.add_argument(
        '--labels-output',
        type=str,
        help='Separate output file for labels (optional)'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )
    
    return parser


def main() -> int:
    """
    Main function for training data generation.
    
    Returns:
        int: Exit code (0 for success, non-zero for failure)
    """
    parser = setup_argparse()
    args = parser.parse_args()
    
    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    logger.info("Starting qlib training data generation")
    logger.info(f"Configuration: region={args.region}, instruments={args.instruments}")
    
    # Initialize data processor
    processor = QlibDataProcessor(
        provider_uri=args.provider_uri,
        region=args.region
    )
    
    # Initialize qlib
    if not processor.initialize():
        logger.error("Failed to initialize qlib. Please ensure qlib is installed and data is available.")
        logger.error("To download qlib data, run: python -m qlib.run.get_data qlib_data --target_dir ~/.qlib/qlib_data/cn_data --region cn")
        return 1
    
    # Prepare training data
    logger.info("Fetching and processing data...")
    features, labels = processor.prepare_training_data(
        instruments=args.instruments,
        start_time=args.start_time,
        end_time=args.end_time,
        generate_features=not args.no_features
    )
    
    if features is None:
        logger.error("Failed to prepare training data")
        return 1
    
    # Save features
    logger.info(f"Saving features to {args.output}")
    if not processor.save_data(features, args.output, args.format):
        logger.error("Failed to save features")
        return 1
    
    # Save labels if separate output specified
    if args.labels_output and labels is not None:
        logger.info(f"Saving labels to {args.labels_output}")
        if not processor.save_data(labels.to_frame(), args.labels_output, args.format):
            logger.error("Failed to save labels")
            return 1
    
    logger.info("Training data generation completed successfully")
    logger.info(f"Features shape: {features.shape}")
    if labels is not None:
        logger.info(f"Labels shape: {labels.shape}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
