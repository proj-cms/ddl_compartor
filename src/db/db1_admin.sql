-- Admin script: create schtest_schemaema1 and grant privileges
-- Run this as a DBA (e.g. CONNECT / AS SYSDBA) because CREATE USER requires elevated privileges.
-- Example: sqlplus / as sysdba

SET SERVEROUTPUT ON
DECLARE
    v_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO v_count FROM all_users WHERE username = UPPER('test_schema');
    IF v_count = 0 THEN
        EXECUTE IMMEDIATE 'CREATE USER test_schema IDENTIFIED BY test1234';
        EXECUTE IMMEDIATE 'GRANT CREATE SESSION TO test_schema';
        EXECUTE IMMEDIATE 'GRANT CREATE TABLE TO test_schema';
        -- Optionally set tablespace/quota:
        -- EXECUTE IMMEDIATE 'ALTER USER test_schema QUOTA UNLIMITED ON USERS';
        DBMS_OUTPUT.PUT_LINE('Created user test_schema and granted CREATE SESSION, CREATE TABLE.');
    ELSE
        DBMS_OUTPUT.PUT_LINE('User test_schema already exists.');
    END IF;
END;
/
