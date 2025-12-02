"""Oracle DDL Comparator - Database Comparison Module

This module provides functionality to compare Data Definition Language (DDL) metadata
between two Oracle databases and generate a detailed comparison report.

The module compares table column definitions including data types, lengths, precision,
scale, and nullability constraints. Results are exported to an Excel file with three sheets:
- DiffColumns: Columns that exist in both databases but have different attributes
- OnlyInDB1: Columns that exist only in the primary (reference) database
- OnlyInDB2: Columns that exist only in the secondary (target) database

Typical usage example:
    from src.db_compare import compare_ddls
    
    # Run comparison with config file
    diff, only_db1, only_db2, result_path = compare_ddls('config.yaml')
    
    # Write results to Excel
    from src.excel.excel_writer import ExcelWriter
    ExcelWriter.write(diff, only_db1, only_db2, result_path)

Configuration:
    The config.yaml file should include:
    - primary_db: Which database to use as reference (oracle_db1 or oracle_db2)
    - oracle_db1: Connection details for first database
    - oracle_db2: Connection details for second database
    - result_excel_path: Output file path (use 'DEFAULT' for auto-generated name)
"""

import yaml
import pandas as pd
import logging
from src.db.oracle_db import OracleDB
from src.excel.excel_writer import ExcelWriter

def compare_ddls(config_path):
    """Compares the DDLs of two Oracle databases.
    
    Reads database connection configurations from a YAML file, connects to both databases,
    retrieves column metadata, and identifies differences. The comparison includes data types,
    lengths, precision, scale, and nullability constraints.

    Args:
        config_path (str): The path to the YAML configuration file containing database
            connection details and comparison settings.

    Returns:
        tuple: A 4-element tuple containing:
            - diff (pd.DataFrame): Columns with differing attributes in both databases
            - only_db1 (pd.DataFrame): Columns only present in the primary database
            - only_db2 (pd.DataFrame): Columns only present in the secondary database
            - result_path (str): The resolved path where Excel results will be written
            
    Raises:
        FileNotFoundError: If the configuration file doesn't exist
        yaml.YAMLError: If the configuration file is malformed
        KeyError: If required configuration keys are missing
        Exception: If database connection or query execution fails
        
    Example:
        >>> diff, only_db1, only_db2, path = compare_ddls('config.yaml')
        >>> print(f"Found {len(diff)} differing columns")
        >>> print(f"Results will be written to: {path}")
    """
    import os
    logger = logging.getLogger(__name__)
    try:
        with open(config_path) as f:
            config = yaml.safe_load(f)
        # Pre-compute result path from config so we can return it even on early exit
        result_path = config.get('result_excel_path', None)
        if not result_path or result_path == 'DEFAULT':
            result_path = os.path.join(os.getcwd(), 'ddl_compare_result.xlsx')
        # Ensure .xlsx extension
        if not result_path.lower().endswith('.xlsx'):
            result_path += '.xlsx'

        # Determine which database is primary
        primary_db = config.get('primary_db', 'oracle_db1')
        if primary_db not in ['oracle_db1', 'oracle_db2']:
            logger.warning(f"Invalid primary_db '{primary_db}' in config. Defaulting to 'oracle_db1'.")
            primary_db = 'oracle_db1'
        
        # Assign databases so primary is always db1 (reference), secondary is db2 (target)
        if primary_db == 'oracle_db1':
            primary_config = config['oracle_db1']
            secondary_config = config['oracle_db2']
        else:
            primary_config = config['oracle_db2']
            secondary_config = config['oracle_db1']
        
        db1 = OracleDB(primary_config)
        logger.info(f"Successfully connected to PRIMARY DB: {primary_config.get('username','<user>')}@{primary_config.get('host')}:{primary_config.get('port')}/{primary_config.get('service_name')}")
        db2 = OracleDB(secondary_config)
        logger.info(f"Successfully connected to SECONDARY DB: {secondary_config.get('username','<user>')}@{secondary_config.get('host')}:{secondary_config.get('port')}/{secondary_config.get('service_name')}")
        df1 = db1.get_columns()
        df2 = db2.get_columns()

        # If any single metadata fetch returned zero rows, log and return immediately
        early_exit = False
        if df1 is None or df1.empty:
            logger.warning(f"No column metadata returned from DB1 ({getattr(db1, 'label', 'db1')}). Aborting comparison.")
            early_exit = True
        if df2 is None or df2.empty:
            logger.warning(f"No column metadata returned from DB2 ({getattr(db2, 'label', 'db2')}). Aborting comparison.")
            early_exit = True
        if early_exit:
            # Return empty diff and the raw frames for inspection, plus the result path
            return pd.DataFrame(), df1, df2, result_path

        merged = pd.merge(df1, df2, on=['table_name', 'column_name'], suffixes=('_db1', '_db2'))
        diff = merged[(merged['data_type_db1'] != merged['data_type_db2']) |
                      (merged['data_length_db1'] != merged['data_length_db2']) |
                      (merged['data_precision_db1'] != merged['data_precision_db2']) |
                      (merged['data_scale_db1'] != merged['data_scale_db2']) |
                      (merged['nullable_db1'] != merged['nullable_db2'])]
        only_db1 = df1[~df1.set_index(['table_name', 'column_name']).index.isin(df2.set_index(['table_name', 'column_name']).index)]
        only_db2 = df2[~df2.set_index(['table_name', 'column_name']).index.isin(df1.set_index(['table_name', 'column_name']).index)]
        logger.info("DDL comparison completed successfully.")
        return diff, only_db1, only_db2, result_path
    except Exception as e:
        logger.error(f"Error in DDL comparison: {e}")
        raise

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)s %(name)s %(message)s')
    try:
        config_path = os.path.join(os.path.dirname(__file__), 'config.yaml')
        diff, only_db1, only_db2, result_path = compare_ddls(config_path)
        ExcelWriter.write(diff, only_db1, only_db2, result_path)
    except Exception as e:
        logging.critical(f"Pipeline failed: {e}")


