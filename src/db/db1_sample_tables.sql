-- Tables-only script for DB1: creates tables in schema1 if they don't exist
-- Run as schema1 (or as a user with permission to create objects in schema1)
SET SERVEROUTPUT ON

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
        DBMS_OUTPUT.PUT_LINE('Created table SCHEMA1.EMP_COMMON');
    ELSE
        DBMS_OUTPUT.PUT_LINE('SCHEMA1.EMP_COMMON already exists');
    END IF;

    SELECT COUNT(*) INTO v_cnt FROM all_tables WHERE owner = 'SCHEMA1' AND table_name = 'DEPT_COMMON';
    IF v_cnt = 0 THEN
        EXECUTE IMMEDIATE q'[CREATE TABLE schema1.dept_common (
            dept_id NUMBER(5) PRIMARY KEY,
            dept_name VARCHAR2(50),
            location VARCHAR2(50),
            budget NUMBER(12,2)
        )]';
        DBMS_OUTPUT.PUT_LINE('Created table SCHEMA1.DEPT_COMMON');
    ELSE
        DBMS_OUTPUT.PUT_LINE('SCHEMA1.DEPT_COMMON already exists');
    END IF;

    SELECT COUNT(*) INTO v_cnt FROM all_tables WHERE owner = 'SCHEMA1' AND table_name = 'ONLY_IN_DB1';
    IF v_cnt = 0 THEN
        EXECUTE IMMEDIATE q'[CREATE TABLE schema1.only_in_db1 (
            id NUMBER(10) PRIMARY KEY,
            description VARCHAR2(100)
        )]';
        DBMS_OUTPUT.PUT_LINE('Created table SCHEMA1.ONLY_IN_DB1');
    ELSE
        DBMS_OUTPUT.PUT_LINE('SCHEMA1.ONLY_IN_DB1 already exists');
    END IF;

    SELECT COUNT(*) INTO v_cnt FROM all_tables WHERE owner = 'SCHEMA1' AND table_name = 'DIFF_TABLE';
    IF v_cnt = 0 THEN
        EXECUTE IMMEDIATE q'[CREATE TABLE schema1.diff_table (
            id NUMBER(10) PRIMARY KEY,
            col1 VARCHAR2(20),
            col2 NUMBER(5,2),
            col3 DATE
        )]';
        DBMS_OUTPUT.PUT_LINE('Created table SCHEMA1.DIFF_TABLE');
    ELSE
        DBMS_OUTPUT.PUT_LINE('SCHEMA1.DIFF_TABLE already exists');
    END IF;
END;
/
-- End of tables-only script for DB1

/*
docker cp src\db\db1_sample_tables.sql ddl_comparator-oracle-db1-1:/tmp/db1_sample_tables.sql
docker exec -it ddl_comparator-oracle-db1-1 bash -c "source /home/oracle/.bashrc; sqlplus schema1/test1234@localhost/XEPDB1 @/tmp/db1_sample_tables.sql"
*/  
