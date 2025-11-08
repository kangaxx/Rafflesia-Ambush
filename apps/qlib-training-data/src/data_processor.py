"""
Data Processor Module

Handles data fetching, processing, and preparation for training using qlib.
"""

import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta

try:
    import qlib
    from qlib.data import D
    from qlib.data.dataset import DatasetH
    from qlib.data.dataset.handler import DataHandlerLP
except ImportError:
    qlib = None
    logging.warning("qlib is not installed. Please install it with: pip install pyqlib")

import pandas as pd


logger = logging.getLogger(__name__)


class QlibDataProcessor:
    """
    Data processor for qlib-based training data generation.
    
    This class handles the initialization of qlib, data fetching,
    and preprocessing for training machine learning models.
    """
    
    def __init__(
        self,
        provider_uri: str = "~/.qlib/qlib_data/cn_data",
        region: str = "cn"
    ):
        """
        Initialize the QlibDataProcessor.
        
        Args:
            provider_uri: Path to qlib data provider
            region: Region for data (e.g., 'cn' for China, 'us' for US)
        """
        self.provider_uri = provider_uri
        self.region = region
        self.initialized = False
        
    def initialize(self) -> bool:
        """
        Initialize qlib with the specified configuration.
        
        Returns:
            bool: True if initialization successful, False otherwise
        """
        if qlib is None:
            logger.error("qlib is not installed")
            return False
            
        try:
            qlib.init(provider_uri=self.provider_uri, region=self.region)
            self.initialized = True
            logger.info(f"Qlib initialized successfully with region: {self.region}")
            return True
        except Exception as e:
            logger.error(f"Failed to initialize qlib: {e}")
            return False
    
    def fetch_data(
        self,
        instruments: str = "csi300",
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        fields: Optional[List[str]] = None
    ) -> Optional[pd.DataFrame]:
        """
        Fetch market data using qlib.
        
        Args:
            instruments: Stock instruments to fetch (e.g., 'csi300', 'all')
            start_time: Start date in format 'YYYY-MM-DD'
            end_time: End date in format 'YYYY-MM-DD'
            fields: List of fields to fetch
            
        Returns:
            pd.DataFrame: Fetched market data, or None if failed
        """
        if not self.initialized:
            logger.error("Qlib not initialized. Call initialize() first.")
            return None
            
        if qlib is None:
            logger.error("qlib is not installed")
            return None
        
        # Default fields if not specified
        if fields is None:
            fields = [
                "$open", "$high", "$low", "$close", "$volume",
                "$factor", "$change", "$vwap"
            ]
        
        # Default time range if not specified
        if start_time is None:
            start_time = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
        if end_time is None:
            end_time = datetime.now().strftime("%Y-%m-%d")
            
        try:
            logger.info(f"Fetching data for {instruments} from {start_time} to {end_time}")
            data = D.features(
                instruments=D.instruments(instruments),
                fields=fields,
                start_time=start_time,
                end_time=end_time
            )
            logger.info(f"Successfully fetched {len(data)} records")
            return data
        except Exception as e:
            logger.error(f"Failed to fetch data: {e}")
            return None
    
    def generate_features(
        self,
        data: pd.DataFrame,
        feature_config: Optional[Dict] = None
    ) -> Optional[pd.DataFrame]:
        """
        Generate additional features from raw data.
        
        Args:
            data: Raw market data
            feature_config: Configuration for feature generation
            
        Returns:
            pd.DataFrame: Data with generated features
        """
        if data is None or data.empty:
            logger.error("Input data is empty")
            return None
            
        try:
            # Add basic technical indicators
            df = data.copy()
            
            # Moving averages
            if "$close" in df.columns:
                df["ma5"] = df["$close"].rolling(window=5).mean()
                df["ma10"] = df["$close"].rolling(window=10).mean()
                df["ma20"] = df["$close"].rolling(window=20).mean()
            
            # Price momentum
            if "$close" in df.columns:
                df["momentum_5"] = df["$close"].pct_change(periods=5)
                df["momentum_10"] = df["$close"].pct_change(periods=10)
            
            # Volatility
            if "$close" in df.columns:
                df["volatility_10"] = df["$close"].rolling(window=10).std()
                df["volatility_20"] = df["$close"].rolling(window=20).std()
            
            logger.info(f"Generated features, new shape: {df.shape}")
            return df
            
        except Exception as e:
            logger.error(f"Failed to generate features: {e}")
            return None
    
    def prepare_training_data(
        self,
        instruments: str = "csi300",
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        fields: Optional[List[str]] = None,
        generate_features: bool = True
    ) -> Tuple[Optional[pd.DataFrame], Optional[pd.Series]]:
        """
        Prepare complete training dataset with features and labels.
        
        Args:
            instruments: Stock instruments to fetch
            start_time: Start date
            end_time: End date
            fields: List of fields to fetch
            generate_features: Whether to generate additional features
            
        Returns:
            Tuple of (features DataFrame, labels Series)
        """
        # Fetch raw data
        data = self.fetch_data(instruments, start_time, end_time, fields)
        if data is None:
            return None, None
        
        # Generate features if requested
        if generate_features:
            data = self.generate_features(data)
            if data is None:
                return None, None
        
        # Generate labels (e.g., next day return)
        try:
            if "$close" in data.columns:
                labels = data["$close"].pct_change(periods=-1)  # Next period return
                labels = labels.shift(-1)  # Align labels
                
                # Remove the last row (no label available)
                data = data[:-1]
                labels = labels[:-1]
                
                logger.info("Training data prepared successfully")
                return data, labels
            else:
                logger.error("Close price not available for label generation")
                return data, None
                
        except Exception as e:
            logger.error(f"Failed to prepare training data: {e}")
            return None, None
    
    def save_data(
        self,
        data: pd.DataFrame,
        output_path: str,
        format: str = "csv"
    ) -> bool:
        """
        Save processed data to file.
        
        Args:
            data: Data to save
            output_path: Output file path
            format: File format ('csv', 'parquet', 'pickle')
            
        Returns:
            bool: True if save successful, False otherwise
        """
        if data is None or data.empty:
            logger.error("No data to save")
            return False
            
        try:
            if format == "csv":
                data.to_csv(output_path)
            elif format == "parquet":
                data.to_parquet(output_path)
            elif format == "pickle":
                data.to_pickle(output_path)
            else:
                logger.error(f"Unsupported format: {format}")
                return False
                
            logger.info(f"Data saved to {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save data: {e}")
            return False
