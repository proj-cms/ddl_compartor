-- Tables-only script for DB1: creates tables in test_schema if they don't exist
-- Run as test_schema (or as a user with permission to create objects in test_schema)
SET SERVEROUTPUT ON

DECLARE
    v_cnt INTEGER;
BEGIN
    SELECT COUNT(*) INTO v_cnt FROM all_tables WHERE owner = 'test_schema' AND table_name = 'EMP_COMMON';
    IF v_cnt = 0 THEN
        EXECUTE IMMEDIATE q'[CREATE TABLE test_schema.emp_common (
            emp_id NUMBER(10) PRIMARY KEY,
            name VARCHAR2(50),
            salary NUMBER(8,2),
            dept_id NUMBER(5)
        )]';
        DBMS_OUTPUT.PUT_LINE('Created table test_schema.EMP_COMMON');
    ELSE
        DBMS_OUTPUT.PUT_LINE('test_schema.EMP_COMMON already exists');
    END IF;

    SELECT COUNT(*) INTO v_cnt FROM all_tables WHERE owner = 'test_schema' AND table_name = 'DEPT_COMMON';
    IF v_cnt = 0 THEN
        EXECUTE IMMEDIATE q'[CREATE TABLE test_schema.dept_common (
            dept_id NUMBER(5) PRIMARY KEY,
            dept_name VARCHAR2(50),
            location VARCHAR2(50),
            budget NUMBER(12,2)
        )]';
        DBMS_OUTPUT.PUT_LINE('Created table test_schema.DEPT_COMMON');
    ELSE
        DBMS_OUTPUT.PUT_LINE('test_schema.DEPT_COMMON already exists');
    END IF;

    SELECT COUNT(*) INTO v_cnt FROM all_tables WHERE owner = 'test_schema' AND table_name = 'ONLY_IN_DB1';
    IF v_cnt = 0 THEN
        EXECUTE IMMEDIATE q'[CREATE TABLE test_schema.only_in_db1 (
            id NUMBER(10) PRIMARY KEY,
            description VARCHAR2(100)
        )]';
        DBMS_OUTPUT.PUT_LINE('Created table test_schema.ONLY_IN_DB1');
    ELSE
        DBMS_OUTPUT.PUT_LINE('test_schema.ONLY_IN_DB1 already exists');
    END IF;

    SELECT COUNT(*) INTO v_cnt FROM all_tables WHERE owner = 'test_schema' AND table_name = 'DIFF_TABLE';
    IF v_cnt = 0 THEN
        EXECUTE IMMEDIATE q'[CREATE TABLE test_schema.diff_table (
            id NUMBER(10) PRIMARY KEY,
            col1 VARCHAR2(20),
            col2 NUMBER(5,2),
            col3 DATE
        )]';
        DBMS_OUTPUT.PUT_LINE('Created table test_schema.DIFF_TABLE');
    ELSE
        DBMS_OUTPUT.PUT_LINE('test_schema.DIFF_TABLE already exists');
    END IF;
END;
/
-- End of tables-only script for DB1

/*
docker cp src\db\db1_sample_tables.sql ddl_comparator-oracle-db1-1:/tmp/db1_sample_tables.sql
docker exec -it ddl_comparator-oracle-db1-1 bash -c "source /home/oracle/.bashrc; sqlplus test_schema/test1234@localhost/XE @/tmp/db1_sample_tables.sql"
*/  
