import unittest
from unittest.mock import patch, MagicMock, Mock
import pandas as pd
import yaml
import os
import sys

# Mock oracledb module before importing from src
sys.modules['oracledb'] = Mock()

from src.db_compare import compare_ddls

class TestDBCompare(unittest.TestCase):

    @patch('src.db_compare.OracleDB')
    @patch('builtins.open')
    def test_compare_ddls_no_diff(self, mock_open, mock_oracle_db):
        # Mock config file
        mock_yaml = {
            'oracle_db1': {'user': 'a'},
            'oracle_db2': {'user': 'b'},
            'result_excel_path': 'test_result.xlsx'
        }
        mock_open.return_value.__enter__.return_value.read.return_value = yaml.dump(mock_yaml)

        # Mock database data
        db_data = {
            'table_name': ['TABLE1', 'TABLE1'],
            'column_name': ['COL1', 'COL2'],
            'data_type': ['VARCHAR2', 'NUMBER'],
            'data_length': [20, 22],
            'data_precision': [None, 10],
            'data_scale': [None, 2],
            'nullable': ['N', 'Y']
        }
        mock_df = pd.DataFrame(db_data)

        mock_db_instance = MagicMock()
        mock_db_instance.get_columns.return_value = mock_df
        mock_oracle_db.return_value = mock_db_instance

        # Run the comparison
        diff, only_db1, only_db2, result_path = compare_ddls('dummy_config.yaml')

        # Assertions
        self.assertTrue(diff.empty)
        self.assertTrue(only_db1.empty)
        self.assertTrue(only_db2.empty)
        self.assertEqual(result_path, 'test_result.xlsx')

    @patch('src.db_compare.OracleDB')
    @patch('builtins.open')
    def test_compare_ddls_with_diff(self, mock_open, mock_oracle_db):
        # Mock config file
        mock_yaml = {
            'oracle_db1': {'user': 'a'},
            'oracle_db2': {'user': 'b'},
            'result_excel_path': 'test_result.xlsx'
        }
        mock_open.return_value.__enter__.return_value.read.return_value = yaml.dump(mock_yaml)

        # Mock database data
        db1_data = {
            'table_name': ['TABLE1', 'TABLE1'],
            'column_name': ['COL1', 'COL2'],
            'data_type': ['VARCHAR2', 'NUMBER'],
            'data_length': [20, 22],
            'data_precision': [None, 10],
            'data_scale': [None, 2],
            'nullable': ['N', 'Y']
        }
        df1 = pd.DataFrame(db1_data)

        db2_data = {
            'table_name': ['TABLE1', 'TABLE1'],
            'column_name': ['COL1', 'COL2'],
            'data_type': ['VARCHAR2', 'INTEGER'],
            'data_length': [30, 22],
            'data_precision': [None, 10],
            'data_scale': [None, 2],
            'nullable': ['Y', 'Y']
        }
        df2 = pd.DataFrame(db2_data)

        # Mock OracleDB
        mock_db1_instance = MagicMock()
        mock_db1_instance.get_columns.return_value = df1
        mock_db2_instance = MagicMock()
        mock_db2_instance.get_columns.return_value = df2

        def side_effect(config):
            if config['user'] == 'a':
                return mock_db1_instance
            return mock_db2_instance
        mock_oracle_db.side_effect = side_effect


        # Run the comparison
        diff, only_db1, only_db2, result_path = compare_ddls('dummy_config.yaml')

        # Assertions
        self.assertEqual(len(diff), 2)
        self.assertTrue(only_db1.empty)
        self.assertTrue(only_db2.empty)

    @patch('src.db_compare.OracleDB')
    @patch('builtins.open')
    def test_compare_ddls_only_in_one_db(self, mock_open, mock_oracle_db):
        # Mock config file
        mock_yaml = {
            'oracle_db1': {'user': 'a'},
            'oracle_db2': {'user': 'b'},
            'result_excel_path': 'test_result.xlsx'
        }
        mock_open.return_value.__enter__.return_value.read.return_value = yaml.dump(mock_yaml)

        # Mock database data
        db1_data = {
            'table_name': ['TABLE1', 'TABLE2'],
            'column_name': ['COL1', 'COL1'],
            'data_type': ['VARCHAR2', 'NUMBER'],
            'data_length': [20, 22],
            'data_precision': [None, 10],
            'data_scale': [None, 2],
            'nullable': ['N', 'Y']
        }
        df1 = pd.DataFrame(db1_data)

        db2_data = {
            'table_name': ['TABLE1'],
            'column_name': ['COL1'],
            'data_type': ['VARCHAR2'],
            'data_length': [20],
            'data_precision': [None],
            'data_scale': [None],
            'nullable': ['N']
        }
        df2 = pd.DataFrame(db2_data)

        # Mock OracleDB
        mock_db1_instance = MagicMock()
        mock_db1_instance.get_columns.return_value = df1
        mock_db2_instance = MagicMock()
        mock_db2_instance.get_columns.return_value = df2
        def side_effect(config):
            if config['user'] == 'a':
                return mock_db1_instance
            return mock_db2_instance
        mock_oracle_db.side_effect = side_effect

        # Run the comparison
        diff, only_db1, only_db2, result_path = compare_ddls('dummy_config.yaml')

        # Assertions
        self.assertTrue(diff.empty)
        self.assertEqual(len(only_db1), 1)
        self.assertTrue(only_db2.empty)
        self.assertEqual(only_db1.iloc[0]['table_name'], 'TABLE2')

    @patch('src.db_compare.OracleDB')
    @patch('builtins.open')
    def test_primary_db_oracle_db1(self, mock_open, mock_oracle_db):
        """Test with oracle_db1 explicitly set as primary"""
        mock_yaml = {
            'primary_db': 'oracle_db1',
            'oracle_db1': {'user': 'a', 'username': 'user1', 'host': 'host1', 'port': 1521, 'service_name': 'XE'},
            'oracle_db2': {'user': 'b', 'username': 'user2', 'host': 'host2', 'port': 1522, 'service_name': 'XE'},
            'result_excel_path': 'test_result.xlsx'
        }
        mock_open.return_value.__enter__.return_value.read.return_value = yaml.dump(mock_yaml)

        db_data = pd.DataFrame({
            'table_name': ['TABLE1'], 'column_name': ['COL1'], 'data_type': ['VARCHAR2'],
            'data_length': [20], 'data_precision': [None], 'data_scale': [None], 'nullable': ['N']
        })

        mock_db1_instance = MagicMock()
        mock_db1_instance.get_columns.return_value = db_data
        mock_db2_instance = MagicMock()
        mock_db2_instance.get_columns.return_value = db_data

        def side_effect(config):
            return mock_db1_instance if config['user'] == 'a' else mock_db2_instance
        mock_oracle_db.side_effect = side_effect

        diff, only_db1, only_db2, result_path = compare_ddls('dummy_config.yaml')
        
        # Verify oracle_db1 was used as primary (first call)
        self.assertEqual(mock_oracle_db.call_args_list[0][0][0]['user'], 'a')
        self.assertTrue(diff.empty)

    @patch('src.db_compare.OracleDB')
    @patch('builtins.open')
    def test_primary_db_oracle_db2(self, mock_open, mock_oracle_db):
        """Test with oracle_db2 set as primary (reversed order)"""
        mock_yaml = {
            'primary_db': 'oracle_db2',
            'oracle_db1': {'user': 'a', 'username': 'user1', 'host': 'host1', 'port': 1521, 'service_name': 'XE'},
            'oracle_db2': {'user': 'b', 'username': 'user2', 'host': 'host2', 'port': 1522, 'service_name': 'XE'},
            'result_excel_path': 'test_result.xlsx'
        }
        mock_open.return_value.__enter__.return_value.read.return_value = yaml.dump(mock_yaml)

        db_data = pd.DataFrame({
            'table_name': ['TABLE1'], 'column_name': ['COL1'], 'data_type': ['VARCHAR2'],
            'data_length': [20], 'data_precision': [None], 'data_scale': [None], 'nullable': ['N']
        })

        mock_db1_instance = MagicMock()
        mock_db1_instance.get_columns.return_value = db_data
        mock_db2_instance = MagicMock()
        mock_db2_instance.get_columns.return_value = db_data

        def side_effect(config):
            return mock_db1_instance if config['user'] == 'a' else mock_db2_instance
        mock_oracle_db.side_effect = side_effect

        diff, only_db1, only_db2, result_path = compare_ddls('dummy_config.yaml')
        
        # Verify oracle_db2 was used as primary (first call should be 'b')
        self.assertEqual(mock_oracle_db.call_args_list[0][0][0]['user'], 'b')
        self.assertTrue(diff.empty)

    @patch('src.db_compare.OracleDB')
    @patch('builtins.open')
    @patch('src.db_compare.logging.getLogger')
    def test_invalid_primary_db(self, mock_logger, mock_open, mock_oracle_db):
        """Test with invalid primary_db value defaults to oracle_db1"""
        mock_yaml = {
            'primary_db': 'invalid_db_name',
            'oracle_db1': {'user': 'a', 'username': 'user1', 'host': 'host1', 'port': 1521, 'service_name': 'XE'},
            'oracle_db2': {'user': 'b', 'username': 'user2', 'host': 'host2', 'port': 1522, 'service_name': 'XE'},
            'result_excel_path': 'test_result.xlsx'
        }
        mock_open.return_value.__enter__.return_value.read.return_value = yaml.dump(mock_yaml)

        db_data = pd.DataFrame({
            'table_name': ['TABLE1'], 'column_name': ['COL1'], 'data_type': ['VARCHAR2'],
            'data_length': [20], 'data_precision': [None], 'data_scale': [None], 'nullable': ['N']
        })

        mock_db_instance = MagicMock()
        mock_db_instance.get_columns.return_value = db_data
        mock_oracle_db.return_value = mock_db_instance

        diff, only_db1, only_db2, result_path = compare_ddls('dummy_config.yaml')
        
        # Verify oracle_db1 was used as default
        self.assertEqual(mock_oracle_db.call_args_list[0][0][0]['user'], 'a')

    @patch('src.db_compare.OracleDB')
    @patch('builtins.open')
    def test_missing_primary_db(self, mock_open, mock_oracle_db):
        """Test missing primary_db defaults to oracle_db1"""
        mock_yaml = {
            # No primary_db field
            'oracle_db1': {'user': 'a', 'username': 'user1', 'host': 'host1', 'port': 1521, 'service_name': 'XE'},
            'oracle_db2': {'user': 'b', 'username': 'user2', 'host': 'host2', 'port': 1522, 'service_name': 'XE'},
            'result_excel_path': 'test_result.xlsx'
        }
        mock_open.return_value.__enter__.return_value.read.return_value = yaml.dump(mock_yaml)

        db_data = pd.DataFrame({
            'table_name': ['TABLE1'], 'column_name': ['COL1'], 'data_type': ['VARCHAR2'],
            'data_length': [20], 'data_precision': [None], 'data_scale': [None], 'nullable': ['N']
        })

        mock_db_instance = MagicMock()
        mock_db_instance.get_columns.return_value = db_data
        mock_oracle_db.return_value = mock_db_instance

        diff, only_db1, only_db2, result_path = compare_ddls('dummy_config.yaml')
        
        # Verify oracle_db1 was used as default
        self.assertEqual(mock_oracle_db.call_args_list[0][0][0]['user'], 'a')

    @patch('src.db_compare.OracleDB')
    @patch('builtins.open')
    def test_empty_db1_dataframe(self, mock_open, mock_oracle_db):
        """Test early exit when db1 returns empty dataframe"""
        mock_yaml = {
            'oracle_db1': {'user': 'a', 'username': 'user1', 'host': 'host1', 'port': 1521, 'service_name': 'XE'},
            'oracle_db2': {'user': 'b', 'username': 'user2', 'host': 'host2', 'port': 1522, 'service_name': 'XE'},
            'result_excel_path': 'test_result.xlsx'
        }
        mock_open.return_value.__enter__.return_value.read.return_value = yaml.dump(mock_yaml)

        empty_df = pd.DataFrame()
        normal_df = pd.DataFrame({
            'table_name': ['TABLE1'], 'column_name': ['COL1'], 'data_type': ['VARCHAR2'],
            'data_length': [20], 'data_precision': [None], 'data_scale': [None], 'nullable': ['N']
        })

        mock_db1_instance = MagicMock()
        mock_db1_instance.get_columns.return_value = empty_df
        mock_db2_instance = MagicMock()
        mock_db2_instance.get_columns.return_value = normal_df

        def side_effect(config):
            return mock_db1_instance if config['user'] == 'a' else mock_db2_instance
        mock_oracle_db.side_effect = side_effect

        diff, only_db1, only_db2, result_path = compare_ddls('dummy_config.yaml')
        
        # Should return empty diff and original dataframes
        self.assertTrue(diff.empty)

    @patch('src.db_compare.OracleDB')
    @patch('builtins.open')
    def test_empty_db2_dataframe(self, mock_open, mock_oracle_db):
        """Test early exit when db2 returns empty dataframe"""
        mock_yaml = {
            'oracle_db1': {'user': 'a', 'username': 'user1', 'host': 'host1', 'port': 1521, 'service_name': 'XE'},
            'oracle_db2': {'user': 'b', 'username': 'user2', 'host': 'host2', 'port': 1522, 'service_name': 'XE'},
            'result_excel_path': 'test_result.xlsx'
        }
        mock_open.return_value.__enter__.return_value.read.return_value = yaml.dump(mock_yaml)

        normal_df = pd.DataFrame({
            'table_name': ['TABLE1'], 'column_name': ['COL1'], 'data_type': ['VARCHAR2'],
            'data_length': [20], 'data_precision': [None], 'data_scale': [None], 'nullable': ['N']
        })
        empty_df = pd.DataFrame()

        mock_db1_instance = MagicMock()
        mock_db1_instance.get_columns.return_value = normal_df
        mock_db2_instance = MagicMock()
        mock_db2_instance.get_columns.return_value = empty_df

        def side_effect(config):
            return mock_db1_instance if config['user'] == 'a' else mock_db2_instance
        mock_oracle_db.side_effect = side_effect

        diff, only_db1, only_db2, result_path = compare_ddls('dummy_config.yaml')
        
        # Should return empty diff
        self.assertTrue(diff.empty)

    @patch('src.db_compare.OracleDB')
    @patch('builtins.open')
    @patch('os.getcwd')
    def test_result_path_default(self, mock_getcwd, mock_open, mock_oracle_db):
        """Test DEFAULT result_excel_path creates ddl_compare_result.xlsx in cwd"""
        mock_getcwd.return_value = '/mock/path'
        mock_yaml = {
            'oracle_db1': {'user': 'a', 'username': 'user1', 'host': 'host1', 'port': 1521, 'service_name': 'XE'},
            'oracle_db2': {'user': 'b', 'username': 'user2', 'host': 'host2', 'port': 1522, 'service_name': 'XE'},
            'result_excel_path': 'DEFAULT'
        }
        mock_open.return_value.__enter__.return_value.read.return_value = yaml.dump(mock_yaml)

        db_data = pd.DataFrame({
            'table_name': ['TABLE1'], 'column_name': ['COL1'], 'data_type': ['VARCHAR2'],
            'data_length': [20], 'data_precision': [None], 'data_scale': [None], 'nullable': ['N']
        })

        mock_db_instance = MagicMock()
        mock_db_instance.get_columns.return_value = db_data
        mock_oracle_db.return_value = mock_db_instance

        diff, only_db1, only_db2, result_path = compare_ddls('dummy_config.yaml')
        
        # Verify default path
        self.assertIn('ddl_compare_result.xlsx', result_path)

    @patch('src.db_compare.OracleDB')
    @patch('builtins.open')
    def test_result_path_no_extension(self, mock_open, mock_oracle_db):
        """Test .xlsx extension is added when missing"""
        mock_yaml = {
            'oracle_db1': {'user': 'a', 'username': 'user1', 'host': 'host1', 'port': 1521, 'service_name': 'XE'},
            'oracle_db2': {'user': 'b', 'username': 'user2', 'host': 'host2', 'port': 1522, 'service_name': 'XE'},
            'result_excel_path': 'my_result'
        }
        mock_open.return_value.__enter__.return_value.read.return_value = yaml.dump(mock_yaml)

        db_data = pd.DataFrame({
            'table_name': ['TABLE1'], 'column_name': ['COL1'], 'data_type': ['VARCHAR2'],
            'data_length': [20], 'data_precision': [None], 'data_scale': [None], 'nullable': ['N']
        })

        mock_db_instance = MagicMock()
        mock_db_instance.get_columns.return_value = db_data
        mock_oracle_db.return_value = mock_db_instance

        diff, only_db1, only_db2, result_path = compare_ddls('dummy_config.yaml')
        
        # Verify .xlsx extension was added
        self.assertTrue(result_path.endswith('.xlsx'))

    @patch('src.db_compare.OracleDB')
    @patch('builtins.open')
    def test_compare_ddls_exception(self, mock_open, mock_oracle_db):
        """Test exception handling in compare_ddls"""
        mock_yaml = {
            'oracle_db1': {'user': 'a', 'username': 'user1', 'host': 'host1', 'port': 1521, 'service_name': 'XE'},
            'oracle_db2': {'user': 'b', 'username': 'user2', 'host': 'host2', 'port': 1522, 'service_name': 'XE'},
            'result_excel_path': 'test_result.xlsx'
        }
        mock_open.return_value.__enter__.return_value.read.return_value = yaml.dump(mock_yaml)

        # Make OracleDB raise an exception
        mock_oracle_db.side_effect = Exception("Database connection failed")

        with self.assertRaises(Exception) as context:
            compare_ddls('dummy_config.yaml')
        
        self.assertIn("Database connection failed", str(context.exception))

if __name__ == '__main__':
    unittest.main()