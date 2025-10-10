import yaml
import pandas as pd
import logging
from db.oracle_db import OracleDB
from excel.excel_writer import ExcelWriter

def compare_ddls(config_path):
    """Compares the DDLs of two Oracle databases.

    Args:
        config_path (str): The path to the configuration file.

    Returns:
        tuple: A tuple containing the differences, tables only in db1, tables only in db2, and the result path.
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

        db1 = OracleDB(config['oracle_db1'])
        logger.info(f"Successfully connected to DB1: {config['oracle_db1'].get('username','<user>')}@{config['oracle_db1'].get('host')}:{config['oracle_db1'].get('port')}/{config['oracle_db1'].get('service_name')}")
        db2 = OracleDB(config['oracle_db2'])
        logger.info(f"Successfully connected to DB2: {config['oracle_db2'].get('username','<user>')}@{config['oracle_db2'].get('host')}:{config['oracle_db2'].get('port')}/{config['oracle_db2'].get('service_name')}")
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


