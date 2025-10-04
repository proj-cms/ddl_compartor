-- Sample SQL for Oracle DB1
-- Creates user/schema `schema1` with password `test1234` (if not exists), grants basic privileges,
-- then creates the sample tables owned by `schema1` only if they don't already exist.
-- NOTE: Running the CREATE USER and GRANT statements requires DBA privileges.

-- 1) PL/SQL block to create the user if it doesn't exist
DECLARE
    v_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO v_count FROM all_users WHERE username = UPPER('schema1');
    IF v_count = 0 THEN
        EXECUTE IMMEDIATE 'CREATE USER schema1 IDENTIFIED BY test1234';
        EXECUTE IMMEDIATE 'GRANT CREATE SESSION TO schema1';
        EXECUTE IMMEDIATE 'GRANT CREATE TABLE TO schema1';
        -- Optionally give quota on USERS tablespace (adjust tablespace name as needed):
        -- EXECUTE IMMEDIATE 'ALTER USER schema1 QUOTA UNLIMITED ON USERS';
    END IF;
END;
/

-- 2) Create tables in schema1 only if they do not already exist.
-- We check ALL_TABLES (owner and table_name are stored in uppercase by default).

DECLARE
    v_cnt INTEGER;
BEGIN
    SELECT COUNT(*) INTO v_cnt FROM all_tables WHERE owner = 'SCHEMA1' AND table_name = 'EMP_COMMON';
    IF v_cnt = 0 THEN
        EXECUTE IMMEDIATE q'[CREATE TABLE schema1.emp_common (
            emp_id NUMBER(10) PRIMARY KEY,
            name VARCHAR2(50),
            salary NUMBER(8,2),
            dept_id NUMBER(5)
        )]';
    END IF;

    SELECT COUNT(*) INTO v_cnt FROM all_tables WHERE owner = 'SCHEMA1' AND table_name = 'DEPT_COMMON';
    IF v_cnt = 0 THEN
        EXECUTE IMMEDIATE q'[CREATE TABLE schema1.dept_common (
            dept_id NUMBER(5) PRIMARY KEY,
            dept_name VARCHAR2(50),
            location VARCHAR2(50),
            budget NUMBER(12,2)
        )]';
    END IF;

    SELECT COUNT(*) INTO v_cnt FROM all_tables WHERE owner = 'SCHEMA1' AND table_name = 'ONLY_IN_DB1';
    IF v_cnt = 0 THEN
        EXECUTE IMMEDIATE q'[CREATE TABLE schema1.only_in_db1 (
            id NUMBER(10) PRIMARY KEY,
            description VARCHAR2(100)
        )]';
    END IF;

    SELECT COUNT(*) INTO v_cnt FROM all_tables WHERE owner = 'SCHEMA1' AND table_name = 'DIFF_TABLE';
    IF v_cnt = 0 THEN
        EXECUTE IMMEDIATE q'[CREATE TABLE schema1.diff_table (
            id NUMBER(10) PRIMARY KEY,
            col1 VARCHAR2(20),
            col2 NUMBER(5,2),
            col3 DATE
        )]';
    END IF;
END;
/

-- End of idempotent creation script
