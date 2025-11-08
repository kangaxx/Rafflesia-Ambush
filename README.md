# Rafflesia-Ambush

**AI Trading Strategy System Framework for Proactively Identifying & Locking Targets**

[中文简介](#chinese-description)

## Overview

Rafflesia-Ambush is a comprehensive multi-language framework for AI-powered trading strategies. The framework consists of multiple specialized application clusters that work together to provide end-to-end capabilities for quantitative trading.

## Framework Architecture

The framework is designed as a modular system with the following components:

```
Rafflesia-Ambush/
├── apps/                      # Application cluster
│   ├── qlib-training-data/   # Training data generation (Python + qlib)
│   ├── [future] ai-strategy/  # AI strategy models
│   ├── [future] kline-predict/# K-line prediction
│   ├── [future] backtesting/  # Backtesting engine
│   └── [future] reporting/    # Report generation
├── scripts/                   # Server operation scripts
└── docs/                      # Framework documentation
```

## Components

### 1. Training Data Generation (qlib-based)
**Status:** ✅ Implemented  
**Language:** Python  
**Location:** `apps/qlib-training-data/`

A qlib-based application for generating high-quality training data for machine learning models. Features include:
- Historical market data fetching via qlib
- Technical indicator generation
- Feature engineering
- Multiple export formats (CSV, Parquet, Pickle)

[Read more →](apps/qlib-training-data/README.md)

### 2. AI Strategy Engine
**Status:** 🚧 Planned  
**Language:** TBD

AI-powered strategy development and optimization.

### 3. K-line Prediction
**Status:** 🚧 Planned  
**Language:** TBD

Advanced K-line (candlestick) pattern prediction using deep learning.

### 4. Backtesting Engine
**Status:** 🚧 Planned  
**Language:** TBD

High-performance backtesting system for strategy validation.

### 5. Report Generation
**Status:** 🚧 Planned  
**Language:** TBD

Automated report generation for trading performance and analytics.

## Quick Start

### Prerequisites

- Python 3.8 or higher
- pip package manager

### Installation

1. Clone the repository:
```bash
git clone https://github.com/kangaxx/Rafflesia-Ambush.git
cd Rafflesia-Ambush
```

2. Install the first app (qlib training data generation):
```bash
cd apps/qlib-training-data
pip install -r requirements.txt
```

3. Download qlib market data:
```bash
# For Chinese market
python -m qlib.run.get_data qlib_data --target_dir ~/.qlib/qlib_data/cn_data --region cn
```

4. Generate training data:
```bash
cd src
python main.py --instruments csi300 --output training_data.csv
```

## Usage

Each application in the framework is designed to be used independently or as part of an integrated pipeline. Refer to individual app documentation for detailed usage instructions.

## Development

### Adding New Apps

To add a new application to the framework:

1. Create a new directory under `apps/`
2. Follow the standard structure:
   ```
   apps/your-app/
   ├── src/           # Source code
   ├── config/        # Configuration files
   ├── tests/         # Unit tests
   ├── docs/          # Documentation
   ├── README.md      # App-specific documentation
   └── requirements.txt or equivalent
   ```
3. Update the main README with the new component

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- [Qlib](https://github.com/microsoft/qlib) - Microsoft's AI-oriented quantitative investment platform
- All contributors to this project

---

## Chinese Description

这是一个由多种编程语言编写的系列APP集群组成的AI交易策略框架，包含但不限于：

- ✅ **AI数据处理** - 基于qlib的训练数据生成Python程序
- 🚧 **AI策略** - AI驱动的策略开发与优化
- 🚧 **K线预测** - 深度学习K线模式预测
- 🚧 **回测引擎** - 高性能策略验证系统
- 🚧 **报告生成** - 自动化交易分析报告
- 🚧 **服务器运维脚本** - 运维自动化工具

目前第一个APP已完成：基于qlib的训练数据生成Python程序，支持历史数据获取、技术指标生成、特征工程等功能。

### 快速开始

```bash
# 克隆仓库
git clone https://github.com/kangaxx/Rafflesia-Ambush.git

# 安装第一个APP
cd Rafflesia-Ambush/apps/qlib-training-data
pip install -r requirements.txt

# 下载市场数据
python -m qlib.run.get_data qlib_data --target_dir ~/.qlib/qlib_data/cn_data --region cn

# 生成训练数据
cd src
python main.py --instruments csi300 --output training_data.csv
```

详细文档请参考 [qlib训练数据生成应用说明](apps/qlib-training-data/README.md)。
