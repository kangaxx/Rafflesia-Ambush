"""
Unit tests for qlib training data generation.

Note: These tests require qlib to be installed and configured.
Some tests may be skipped if qlib data is not available.
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
import numpy as np
import sys
import os

# Add src directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from data_processor import QlibDataProcessor


class TestQlibDataProcessor(unittest.TestCase):
    """Test cases for QlibDataProcessor class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.processor = QlibDataProcessor(
            provider_uri="~/.qlib/qlib_data/cn_data",
            region="cn"
        )
    
    def test_initialization(self):
        """Test processor initialization."""
        self.assertIsNotNone(self.processor)
        self.assertEqual(self.processor.region, "cn")
        self.assertFalse(self.processor.initialized)
    
    def test_generate_features_with_valid_data(self):
        """Test feature generation with valid data."""
        # Create sample data
        dates = pd.date_range('2023-01-01', periods=30, freq='D')
        data = pd.DataFrame({
            '$close': np.random.randn(30).cumsum() + 100,
            '$open': np.random.randn(30).cumsum() + 100,
            '$volume': np.random.randint(1000, 10000, 30)
        }, index=dates)
        
        # Generate features
        result = self.processor.generate_features(data)
        
        # Verify features were added
        self.assertIsNotNone(result)
        self.assertIn('ma5', result.columns)
        self.assertIn('ma10', result.columns)
        self.assertIn('ma20', result.columns)
        self.assertIn('momentum_5', result.columns)
        self.assertIn('momentum_10', result.columns)
        self.assertIn('volatility_10', result.columns)
        self.assertIn('volatility_20', result.columns)
    
    def test_generate_features_with_empty_data(self):
        """Test feature generation with empty data."""
        data = pd.DataFrame()
        result = self.processor.generate_features(data)
        self.assertIsNone(result)
    
    def test_generate_features_with_none(self):
        """Test feature generation with None input."""
        result = self.processor.generate_features(None)
        self.assertIsNone(result)
    
    def test_save_data_csv(self):
        """Test saving data to CSV format."""
        import tempfile
        
        # Create sample data
        data = pd.DataFrame({
            'value': [1, 2, 3, 4, 5]
        })
        
        # Save to temporary file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            temp_path = f.name
        
        try:
            result = self.processor.save_data(data, temp_path, format='csv')
            self.assertTrue(result)
            
            # Verify file was created
            self.assertTrue(os.path.exists(temp_path))
            
            # Verify data can be read back
            loaded_data = pd.read_csv(temp_path, index_col=0)
            pd.testing.assert_frame_equal(data, loaded_data)
        finally:
            # Clean up
            if os.path.exists(temp_path):
                os.remove(temp_path)
    
    def test_save_data_with_none(self):
        """Test saving None data."""
        result = self.processor.save_data(None, 'output.csv', format='csv')
        self.assertFalse(result)
    
    def test_save_data_with_empty_dataframe(self):
        """Test saving empty DataFrame."""
        data = pd.DataFrame()
        result = self.processor.save_data(data, 'output.csv', format='csv')
        self.assertFalse(result)
    
    def test_save_data_unsupported_format(self):
        """Test saving data with unsupported format."""
        data = pd.DataFrame({'value': [1, 2, 3]})
        result = self.processor.save_data(data, 'output.xyz', format='xyz')
        self.assertFalse(result)


class TestDataProcessorIntegration(unittest.TestCase):
    """Integration tests that require qlib to be installed."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.processor = QlibDataProcessor(
            provider_uri="~/.qlib/qlib_data/cn_data",
            region="cn"
        )
    
    @unittest.skipIf(
        not os.path.exists(os.path.expanduser("~/.qlib/qlib_data/cn_data")),
        "Qlib data not available"
    )
    def test_full_workflow(self):
        """Test complete data generation workflow (requires qlib data)."""
        import tempfile
        
        # Initialize qlib
        if not self.processor.initialize():
            self.skipTest("Could not initialize qlib")
        
        # Prepare training data with a small date range
        features, labels = self.processor.prepare_training_data(
            instruments='csi300',
            start_time='2023-01-01',
            end_time='2023-01-31',
            generate_features=True
        )
        
        # Skip if no data available
        if features is None:
            self.skipTest("Could not fetch data from qlib")
        
        # Verify data structure
        self.assertIsInstance(features, pd.DataFrame)
        self.assertGreater(len(features), 0)
        
        if labels is not None:
            self.assertIsInstance(labels, pd.Series)
            self.assertEqual(len(features), len(labels))
        
        # Test saving
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            temp_path = f.name
        
        try:
            result = self.processor.save_data(features, temp_path, format='csv')
            self.assertTrue(result)
            self.assertTrue(os.path.exists(temp_path))
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)


if __name__ == '__main__':
    unittest.main()
