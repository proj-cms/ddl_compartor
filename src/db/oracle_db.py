import oracledb
import pandas as pd
import logging
import time

from typing import Dict, Any

class OracleDB:
    """A class to connect to an Oracle database and retrieve column metadata."""
    def __init__(self, db_conf: Dict[str, Any]) -> None:
        """Initializes the OracleDB object.

        Args:
            db_conf (Dict[str, Any]): A dictionary containing the database connection configuration.
        """
        self.logger = logging.getLogger(__name__)
        self.db_conf = db_conf
        self.label = db_conf.get('label') or f"{db_conf.get('user')}"
        schemas = db_conf.get('schemas') or db_conf.get('schema')
        if isinstance(schemas, str):
            schemas = [schemas]
        if not schemas:
            schemas = ['TEST_SCHEMA']
        self.schemas = [s.upper() for s in schemas]
        self.retry_count = db_conf.get('retry_count', 3)
        self.retry_delay = db_conf.get('retry_delay', 5)
        self.connection = self._connect_with_retry()

    def _connect_with_retry(self) -> Any:
        """Connects to the Oracle database with a retry mechanism.

        Returns:
            Any: The connection object.
        """
        attempt = 0
        while attempt < self.retry_count:
            try:
                self.logger.info(f"Attempting Oracle DB connection to {self.label} (try {attempt+1}/{self.retry_count})...")
                connection = oracledb.connect(
                    user=self.db_conf['user'],
                    password=self.db_conf['password'],
                    host=self.db_conf['host'],
                    port=self.db_conf['port'],
                    service_name=self.db_conf['service_name']
                )
                self.logger.info(f"Oracle DB connection established for {self.label}.")
                return connection
            except Exception as e:
                self.logger.error(f"Oracle DB connection failed for {self.label}: {e}")
                attempt += 1
                if attempt < self.retry_count:
                    self.logger.info(f"Retrying in {self.retry_delay} seconds...")
                    time.sleep(self.retry_delay)
                else:
                    self.logger.critical(f"Max retries reached. Could not connect to Oracle DB: {self.label}.")
                    raise

    def get_columns(self) -> 'pd.DataFrame':
        """Retrieves the column metadata from the database.

        Returns:
            pd.DataFrame: A pandas DataFrame containing the column metadata.
        """
        owner_list = ', '.join(f"'{s}'" for s in self.schemas)
        sql = f"""
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
        AND owner IN ({owner_list})
        """
        try:
            self.logger.info(f"Fetching column metadata from {self.label} for schemas: {self.schemas}...")
            result = pd.read_sql(sql, self.connection)
            self.logger.info(f"Fetched {len(result)} rows of column metadata from {self.label}.")
            self.logger.debug(f"Columns returned from {self.label}: {result.columns.tolist()}")
            self.logger.debug(f"Sample data from {self.label}: {result.head(5).to_dict()}")
            return result
        except Exception as e:
            self.logger.error(f"Error fetching columns from {self.label}: {e}")
            raise