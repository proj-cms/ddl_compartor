import cx_Oracle
import pandas as pd
import logging
import time
from sqlalchemy import create_engine

from typing import Dict, Any

class OracleDB:
    def __init__(self, db_conf: Dict[str, Any]) -> None:
        self.logger = logging.getLogger(__name__)
        self.dsn = cx_Oracle.makedsn(db_conf['host'], db_conf['port'], service_name=db_conf['service_name'])
        self.retry_count = db_conf.get('retry_count', 3)
        self.retry_delay = db_conf.get('retry_delay', 5)
        self.engine = self._connect_with_retry(db_conf)

    def _connect_with_retry(self, db_conf: Dict[str, Any]) -> 'sqlalchemy.engine.Engine':
        attempt = 0
        connect_str = f"oracle+cx_oracle://{db_conf['username']}:{db_conf['password']}@{db_conf['host']}:{db_conf['port']}/?service_name={db_conf['service_name']}"
        while attempt < self.retry_count:
            try:
                self.logger.info(f"Attempting Oracle DB connection (try {attempt+1}/{self.retry_count})...")
                engine = create_engine(connect_str)
                # Test connection
                with engine.connect() as conn:
                    pass
                self.logger.info("Oracle DB connection established via SQLAlchemy.")
                return engine
            except Exception as e:
                self.logger.error(f"Oracle DB connection failed: {e}")
                attempt += 1
                if attempt < self.retry_count:
                    self.logger.info(f"Retrying in {self.retry_delay} seconds...")
                    time.sleep(self.retry_delay)
                else:
                    self.logger.critical("Max retries reached. Could not connect to Oracle DB.")
                    raise

    def get_columns(self) -> 'pd.DataFrame':
        sql = """
        SELECT owner, table_name, column_name, data_type, data_length, data_precision, data_scale, nullable
        FROM all_tab_columns
        WHERE owner NOT IN (
            'SYS', 'SYSTEM', 'OUTLN', 'XDB', 'DBSNMP', 'APPQOSSYS', 'AUDSYS', 'CTXSYS', 'DVSYS', 'GGSYS', 'GSMADMIN_INTERNAL',
            'LBACSYS', 'MDSYS', 'OJVMSYS', 'OLAPSYS', 'ORDPLUGINS', 'ORDSYS', 'SI_INFORMTN_SCHEMA', 'WMSYS', 'DIP', 'APEX_040000',
            'APEX_050000', 'APEX_180200', 'APEX_210100', 'FLOWS_FILES', 'ANONYMOUS', 'XS$NULL', 'SPATIAL_CSW_ADMIN_USR',
            'SPATIAL_WFS_ADMIN_USR', 'PUBLIC', 'PERFSTAT', 'AUDIT_ADMIN', 'AUDIT_VIEWER', 'ORACLE_OCM', 'REMOTE_SCHEDULER_AGENT',
            'DBSFWUSER', 'SYSDG', 'SYSKM', 'SYSRAC', 'SYSBACKUP', 'MGMT_VIEW', 'SQLTXPLAIN', 'DVSYS', 'DVF', 'GSMCATUSER',
            'GSMUSER', 'GSMROOTUSER', 'GSMREGUSER', 'GSMADMIN_INTERNAL', 'CDB$ROOT', 'PDB$SEED', 'ORACLE_OCM', 'XS$NULL'
        )
        """
        try:
            self.logger.info("Fetching column metadata from Oracle DB...")
            result = pd.read_sql(sql, self.engine)
            self.logger.info(f"Fetched {len(result)} rows of column metadata.")
            self.logger.info(f"Columns returned: {result.columns.tolist()}")
            self.logger.info(f"Sample data: {result.head(5).to_dict()}")
            return result
        except Exception as e:
            self.logger.error(f"Error fetching columns: {e}")
            raise
