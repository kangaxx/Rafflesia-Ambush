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
