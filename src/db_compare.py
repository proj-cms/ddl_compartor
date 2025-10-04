import yaml
import pandas as pd
import logging
from db.oracle_db import OracleDB
from excel.excel_writer import ExcelWriter

def compare_ddls(config_path):
    import os
    logger = logging.getLogger(__name__)
    try:
        with open(config_path) as f:
            config = yaml.safe_load(f)
        db1 = OracleDB(config['oracle_db1'])
        db2 = OracleDB(config['oracle_db2'])
        df1 = db1.get_columns()
        df2 = db2.get_columns()
        merged = pd.merge(df1, df2, on=['table_name', 'column_name'], suffixes=('_db1', '_db2'))
        diff = merged[(merged['data_type_db1'] != merged['data_type_db2']) |
                      (merged['data_length_db1'] != merged['data_length_db2']) |
                      (merged['data_precision_db1'] != merged['data_precision_db2']) |
                      (merged['data_scale_db1'] != merged['data_scale_db2']) |
                      (merged['nullable_db1'] != merged['nullable_db2'])]
        only_db1 = df1[~df1.set_index(['table_name', 'column_name']).index.isin(df2.set_index(['table_name', 'column_name']).index)]
        only_db2 = df2[~df2.set_index(['table_name', 'column_name']).index.isin(df1.set_index(['table_name', 'column_name']).index)]
        logger.info("DDL comparison completed successfully.")
        # Get result path from config, default to current dir
        result_path = config.get('result_excel_path', None)
        if not result_path or result_path == 'DEFAULT':
            result_path = os.path.join(os.getcwd(), 'ddl_compare_result.xlsx')
        # Ensure .xlsx extension
        if not result_path.lower().endswith('.xlsx'):
            result_path += '.xlsx'
        return diff, only_db1, only_db2, result_path
    except Exception as e:
        logger.error(f"Error in DDL comparison: {e}")
        raise

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)s %(name)s %(message)s')
    try:
        diff, only_db1, only_db2, result_path = compare_ddls('src/config.yaml')
        ExcelWriter.write(diff, only_db1, only_db2, result_path)
    except Exception as e:
        logging.critical(f"Pipeline failed: {e}")
