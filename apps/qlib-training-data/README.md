# Qlib Training Data Generation App

This is the first application in the Rafflesia-Ambush framework - a Python-based tool for generating training data using [qlib](https://github.com/microsoft/qlib) for AI trading strategy development.

## Overview

This app provides functionality to:
- Fetch historical market data using qlib
- Generate technical indicators and features
- Prepare training datasets for machine learning models
- Export data in multiple formats (CSV, Parquet, Pickle)

## Prerequisites

### Install Qlib

```bash
pip install pyqlib
```

### Download Market Data

Before using this app, you need to download qlib market data:

```bash
# For Chinese market data (CSI300, etc.)
python -m qlib.run.get_data qlib_data --target_dir ~/.qlib/qlib_data/cn_data --region cn

# For US market data
python -m qlib.run.get_data qlib_data --target_dir ~/.qlib/qlib_data/us_data --region us
```

## Installation

### From Source

```bash
cd apps/qlib-training-data
pip install -r requirements.txt
```

Or install in development mode:

```bash
pip install -e .
```

### With Full Features

To install with all optional dependencies:

```bash
pip install -e ".[full]"
```

## Usage

### Basic Usage

Generate training data for CSI300 stocks:

```bash
cd apps/qlib-training-data/src
python main.py --instruments csi300 --output training_data.csv
```

### Advanced Options

```bash
python main.py \
    --provider-uri ~/.qlib/qlib_data/cn_data \
    --region cn \
    --instruments csi300 \
    --start-time 2020-01-01 \
    --end-time 2023-12-31 \
    --output features.csv \
    --labels-output labels.csv \
    --format csv \
    --verbose
```

### Command-Line Options

- `--provider-uri`: Path to qlib data provider (default: `~/.qlib/qlib_data/cn_data`)
- `--region`: Data region - `cn` or `us` (default: `cn`)
- `--instruments`: Stock instruments to fetch (default: `csi300`)
- `--start-time`: Start date in YYYY-MM-DD format
- `--end-time`: End date in YYYY-MM-DD format
- `--output`: Output file path (default: `training_data.csv`)
- `--format`: Output format - `csv`, `parquet`, or `pickle` (default: `csv`)
- `--no-features`: Skip feature generation, only fetch raw data
- `--labels-output`: Separate output file for labels (optional)
- `--verbose`: Enable verbose logging

## Generated Features

The app automatically generates the following features from raw data:

### Moving Averages
- MA5: 5-day moving average of close price

## Futures Data Downloader

The app also includes a specialized tool for downloading futures data using qlib. This tool allows you to fetch futures contracts with customizable parameters.

### Usage

#### Basic Usage

Download data for multiple futures contracts:

```bash
cd apps/qlib-training-data/src
python qlib_download_future_data.py --contracts CU,AL,ZN
```

#### Custom Output Path

Save data to a specific directory:

```bash
python qlib_download_future_data.py --contracts RU,BU --output-path ./data/futures
```

#### Different Time Frequencies

Download intraday data with specific frequency:

```bash
python qlib_download_future_data.py --contracts IF,IC --freq 5min
```

#### Date Range Selection

Specify start and end dates:

```bash
python qlib_download_future_data.py --contracts SC,MA --start-date 2023-01-01 --end-date 2023-12-31
```

### Command-Line Options

- `--output-path`: Path to save the downloaded futures data (default: `~/.qlib/qlib_data/cn_future`)
- `--contracts`: Future contracts to download, separated by commas (required, e.g., "CU,AL,ZN")
- `--freq`: Data frequency (default: `1d`, choices: `1min`, `5min`, `15min`, `30min`, `60min`, `1d`)
- `--start-date`: Start date in YYYY-MM-DD format
- `--end-date`: End date in YYYY-MM-DD format
- `--fields`: Data fields to download, separated by commas (default: `open,high,low,close,volume,amount`)
- `--region`: Data region (default: `cn`, choices: `cn`, `us`)
- `--verbose`: Enable verbose logging
- `--resume`: Resume download from previous checkpoint if available
- `--chunk-size`: Number of days to download in each chunk (default: 30)

### Output Formats

The tool automatically saves data in two formats:
- CSV format: Easy to view and edit
- Parquet format: Better performance and compression (requires pyarrow)

### Examples

For more detailed examples, please refer to the example script:

```bash
cd apps/qlib-training-data/examples
python usage_examples.py
```

This will display various usage patterns and integration workflows for the futures data downloader.
- MA10: 10-day moving average of close price
- MA20: 20-day moving average of close price

### Momentum Indicators
- momentum_5: 5-day price momentum (percentage change)
- momentum_10: 10-day price momentum

### Volatility Measures
- volatility_10: 10-day rolling standard deviation of close price
- volatility_20: 20-day rolling standard deviation of close price

### Labels
- Next day return: Percentage change in close price for the next trading day

## Configuration

You can customize the data generation process by editing `config/default_config.yaml`.

## Python API

You can also use the data processor programmatically:

```python
from data_processor import QlibDataProcessor

# Initialize processor
processor = QlibDataProcessor(
    provider_uri='~/.qlib/qlib_data/cn_data',
    region='cn'
)

# Initialize qlib
processor.initialize()

# Prepare training data
features, labels = processor.prepare_training_data(
    instruments='csi300',
    start_time='2020-01-01',
    end_time='2023-12-31',
    generate_features=True
)

# Save data
processor.save_data(features, 'features.csv', format='csv')
```

## Output Format

The generated training data includes:

### Features (Raw Data)
- `$open`: Opening price
- `$high`: Highest price
- `$low`: Lowest price
- `$close`: Closing price
- `$volume`: Trading volume
- `$factor`: Adjustment factor
- `$change`: Price change
- `$vwap`: Volume-weighted average price

### Generated Features
- Moving averages (MA5, MA10, MA20)
- Momentum indicators
- Volatility measures

### Labels
- Next day returns (for supervised learning)

## Examples

### Example 1: Generate Data for Last Year

```bash
python main.py \
    --instruments csi300 \
    --output last_year_data.csv
```

### Example 2: Custom Date Range with Parquet Output

```bash
python main.py \
    --instruments csi300 \
    --start-time 2022-01-01 \
    --end-time 2023-12-31 \
    --output training_data.parquet \
    --format parquet
```

### Example 3: Raw Data Only (No Feature Generation)

```bash
python main.py \
    --instruments csi300 \
    --no-features \
    --output raw_data.csv
```

## Troubleshooting

### Qlib Not Initialized

If you see "Failed to initialize qlib", make sure you have:
1. Installed pyqlib: `pip install pyqlib`
2. Downloaded market data (see Prerequisites section)

### No Data Available

If no data is fetched:
1. Check that the data directory exists and contains data
2. Verify the date range is valid
3. Ensure the instruments parameter is correct (e.g., 'csi300' for Chinese stocks)

## Contributing

This is part of the larger Rafflesia-Ambush framework. Contributions are welcome!

## License

MIT License - See the main repository LICENSE file for details.
