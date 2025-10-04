-- Sample SQL for Oracle DB2
CREATE TABLE emp_common (
    emp_id NUMBER(10) PRIMARY KEY,
    name VARCHAR2(50),
    salary NUMBER(10,2), -- Different precision
    dept_id NUMBER(5)
);

CREATE TABLE dept_common (
    dept_id NUMBER(5) PRIMARY KEY,
    dept_name VARCHAR2(50),
    location VARCHAR2(50),
    budget NUMBER(12,2)
);

CREATE TABLE only_in_db2 (
    id NUMBER(10) PRIMARY KEY,
    description VARCHAR2(100)
);

-- Table with column difference
CREATE TABLE diff_table (
    id NUMBER(10) PRIMARY KEY,
    col1 VARCHAR2(20),
    col2 NUMBER(8,2), -- Different precision
    col4 VARCHAR2(30) -- Different column
);

/*
# Copy the SQL file into the container
docker cp src\db\db2_sample_tables.sql ddl_comparator-oracle-db2-1:/tmp/db2_sample_tables.sql

# Run the SQL script inside the container
docker exec -it ddl_comparator-oracle-db2-1 bash -c "source /home/oracle/.bashrc; sqlplus user2/pass2@localhost/XEPDB1 @/tmp/db2_sample_tables.sql"
*/

/*
docker exec -it ddl_comparator-oracle-db2-1 bash
*/
