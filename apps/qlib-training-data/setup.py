"""
Setup script for qlib-training-data app
"""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="qlib-training-data",
    version="0.1.0",
    author="Rafflesia-Ambush Team",
    description="Qlib-based training data generation for AI trading strategies",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/kangaxx/Rafflesia-Ambush",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: Financial and Insurance Industry",
        "Topic :: Office/Business :: Financial :: Investment",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.8",
    install_requires=[
        "pyqlib>=0.9.0",
        "pandas>=1.3.0",
        "numpy>=1.20.0",
    ],
    extras_require={
        "dev": [
            "pytest>=6.0",
            "pytest-cov>=2.0",
            "black>=22.0",
            "flake8>=4.0",
            "mypy>=0.950",
        ],
        "full": [
            "scikit-learn>=1.0.0",
            "matplotlib>=3.3.0",
            "seaborn>=0.11.0",
            "pyarrow>=6.0.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "qlib-training-data=main:main",
        ],
    },
)
