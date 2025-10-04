-- Admin script: create schema1 and grant privileges
-- Run this as a DBA (e.g. CONNECT / AS SYSDBA) because CREATE USER requires elevated privileges.
-- Example: sqlplus / as sysdba

SET SERVEROUTPUT ON
DECLARE
    v_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO v_count FROM all_users WHERE username = UPPER('schema1');
    IF v_count = 0 THEN
        EXECUTE IMMEDIATE 'CREATE USER schema1 IDENTIFIED BY test1234';
        EXECUTE IMMEDIATE 'GRANT CREATE SESSION TO schema1';
        EXECUTE IMMEDIATE 'GRANT CREATE TABLE TO schema1';
        -- Optionally set tablespace/quota:
        -- EXECUTE IMMEDIATE 'ALTER USER schema1 QUOTA UNLIMITED ON USERS';
        DBMS_OUTPUT.PUT_LINE('Created user SCHEMA1 and granted CREATE SESSION, CREATE TABLE.');
    ELSE
        DBMS_OUTPUT.PUT_LINE('User SCHEMA1 already exists.');
    END IF;
END;
/
