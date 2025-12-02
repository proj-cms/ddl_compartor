"""Oracle Database Connection and Metadata Retrieval Module

This module provides the OracleDB class for connecting to Oracle databases with retry logic
and retrieving table column metadata. It handles connection failures gracefully with
configurable retry mechanisms and filters out Oracle system schemas automatically.

Typical usage example:
    from src.db.oracle_db import OracleDB
    
    db_config = {
        'user': 'my_user',
        'password': 'my_password',
        'host': 'localhost',
        'port': 1521,
        'service_name': 'XE',
        'retry_count': 3,
        'retry_delay': 5
    }
    
    db = OracleDB(db_config)
    columns_df = db.get_columns()
    print(f"Retrieved {len(columns_df)} columns")
"""

import oracledb
import pandas as pd
import logging
import time

from typing import Dict, Any

class OracleDB:
    """A class to connect to an Oracle database and retrieve column metadata.
    
    This class manages Oracle database connections with automatic retry logic and provides
    methods to query table column metadata while automatically filtering out system schemas.
    
    Attributes:
        logger (logging.Logger): Logger instance for this class
        db_conf (Dict[str, Any]): Database configuration dictionary
        label (str): Human-readable label for this database connection
        schemas (List[str]): List of schemas to include in queries (uppercase)
        retry_count (int): Maximum number of connection retry attempts
        retry_delay (int): Seconds to wait between retry attempts
        connection: Oracle database connection object
    """
    
    def __init__(self, db_conf: Dict[str, Any]) -> None:
        """Initializes the OracleDB object and establishes database connection.
        
        Processes the configuration, sets up retry parameters, normalizes schema names,
        and attempts to connect to the database with retry logic.

        Args:
            db_conf (Dict[str, Any]): A dictionary containing the database connection configuration.
                Required keys: user, password, host, port, service_name
                Optional keys: label, schema/schemas, retry_count (default: 3), retry_delay (default: 5)
                
        Raises:
            KeyError: If required configuration keys are missing
            Exception: If connection fails after all retry attempts
            
        Example:
            >>> config = {'user': 'test', 'password': 'pass', 'host': 'localhost', 
            ...           'port': 1521, 'service_name': 'XE'}
            >>> db = OracleDB(config)
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
        
        Attempts to establish a database connection with exponential backoff between retries.
        Logs all connection attempts and failures.

        Returns:
            Any: The Oracle database connection object.
            
        Raises:
            Exception: If max retries are exceeded without successful connection
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
        
        Queries the all_tab_columns system view to get column definitions for all tables
        in the configured schemas. Automatically filters out Oracle system schemas.

        Returns:
            pd.DataFrame: A pandas DataFrame containing the column metadata with columns:
                - owner: Schema name
                - table_name: Table name
                - column_name: Column name
                - data_type: Oracle data type (VARCHAR2, NUMBER, etc.)
                - data_length: Maximum length for character types
                - data_precision: Numeric precision
                - data_scale: Numeric scale
                - nullable: 'Y' if nullable, 'N' if NOT NULL
                
        Raises:
            Exception: If the SQL query execution fails
            
        Example:
            >>> db = OracleDB(config)
            >>> df = db.get_columns()
            >>> print(df[['table_name', 'column_name', 'data_type']].head())
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