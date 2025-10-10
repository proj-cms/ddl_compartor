import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from src.db_compare import compare_ddls
import yaml

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

if __name__ == '__main__':
    unittest.main()