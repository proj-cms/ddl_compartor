import unittest
from unittest.mock import patch, MagicMock, Mock
import pandas as pd
import sys

# Mock oracledb module before importing OracleDB
sys.modules['oracledb'] = Mock()

from src.db.oracle_db import OracleDB


class TestOracleDB(unittest.TestCase):

    def setUp(self):
        """Set up test fixtures"""
        self.db_conf = {
            'user': 'test_user',
            'password': 'test_pass',
            'host': 'localhost',
            'port': 1521,
            'service_name': 'XE',
            'retry_count': 3,
            'retry_delay': 1
        }

    @patch('src.db.oracle_db.oracledb.connect')
    def test_connection_success(self, mock_connect):
        """Test successful database connection"""
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection

        db = OracleDB(self.db_conf)

        # Verify connection was attempted
        mock_connect.assert_called_once()
        self.assertIsNotNone(db.connection)

    @patch('src.db.oracle_db.oracledb.connect')
    @patch('src.db.oracle_db.time.sleep')
    def test_connection_retry_success(self, mock_sleep, mock_connect):
        """Test successful connection after retry"""
        mock_connection = MagicMock()
        # Fail first attempt, succeed on second
        mock_connect.side_effect = [Exception("Connection failed"), mock_connection]

        db = OracleDB(self.db_conf)

        # Verify retry happened
        self.assertEqual(mock_connect.call_count, 2)
        self.assertIsNotNone(db.connection)
        mock_sleep.assert_called_once_with(1)

    @patch('src.db.oracle_db.oracledb.connect')
    @patch('src.db.oracle_db.time.sleep')
    def test_connection_max_retries(self, mock_sleep, mock_connect):
        """Test max retries exceeded"""
        # All attempts fail
        mock_connect.side_effect = Exception("Connection failed")

        with self.assertRaises(Exception) as context:
            OracleDB(self.db_conf)

        # Verify all retries were attempted
        self.assertEqual(mock_connect.call_count, 3)
        self.assertIn("Connection failed", str(context.exception))

    @patch('src.db.oracle_db.oracledb.connect')
    @patch('src.db.oracle_db.pd.read_sql')
    def test_get_columns_success(self, mock_read_sql, mock_connect):
        """Test successful column retrieval"""
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection

        # Mock dataframe response
        expected_df = pd.DataFrame({
            'owner': ['TEST_SCHEMA'],
            'table_name': ['TABLE1'],
            'column_name': ['COL1'],
            'data_type': ['VARCHAR2'],
            'data_length': [20],
            'data_precision': [None],
            'data_scale': [None],
            'nullable': ['N']
        })
        mock_read_sql.return_value = expected_df

        db = OracleDB(self.db_conf)
        result = db.get_columns()

        # Verify result
        self.assertIsNotNone(result)
        self.assertEqual(len(result), 1)
        mock_read_sql.assert_called_once()

    @patch('src.db.oracle_db.oracledb.connect')
    @patch('src.db.oracle_db.pd.read_sql')
    def test_get_columns_exception(self, mock_read_sql, mock_connect):
        """Test exception during column retrieval"""
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection

        # Make read_sql raise an exception
        mock_read_sql.side_effect = Exception("Query failed")

        db = OracleDB(self.db_conf)

        with self.assertRaises(Exception) as context:
            db.get_columns()

        self.assertIn("Query failed", str(context.exception))

    @patch('src.db.oracle_db.oracledb.connect')
    @patch('src.db.oracle_db.pd.read_sql')
    def test_schema_filtering(self, mock_read_sql, mock_connect):
        """Test schema filtering logic"""
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection

        # Mock dataframe response
        mock_read_sql.return_value = pd.DataFrame()

        # Test with specific schema
        conf_with_schema = self.db_conf.copy()
        conf_with_schema['schema'] = 'MY_SCHEMA'
        
        db = OracleDB(conf_with_schema)
        db.get_columns()

        # Verify SQL contains the schema filter
        call_args = mock_read_sql.call_args
        sql_query = call_args[0][0]
        self.assertIn('MY_SCHEMA', sql_query)

    @patch('src.db.oracle_db.oracledb.connect')
    def test_multiple_schemas(self, mock_connect):
        """Test handling of multiple schemas"""
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection

        # Test with multiple schemas
        conf_with_schemas = self.db_conf.copy()
        conf_with_schemas['schemas'] = ['SCHEMA1', 'SCHEMA2']
        
        db = OracleDB(conf_with_schemas)

        # Verify schemas are uppercase
        self.assertEqual(db.schemas, ['SCHEMA1', 'SCHEMA2'])

    @patch('src.db.oracle_db.oracledb.connect')
    def test_default_schema(self, mock_connect):
        """Test default schema when none specified"""
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection

        db = OracleDB(self.db_conf)

        # Verify default schema
        self.assertEqual(db.schemas, ['TEST_SCHEMA'])


if __name__ == '__main__':
    unittest.main()
