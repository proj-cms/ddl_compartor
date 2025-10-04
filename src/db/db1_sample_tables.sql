-- Sample SQL for Oracle DB1
CREATE TABLE emp_common (
    emp_id NUMBER(10) PRIMARY KEY,
    name VARCHAR2(50),
    salary NUMBER(8,2),
    dept_id NUMBER(5)
);

CREATE TABLE dept_common (
    dept_id NUMBER(5) PRIMARY KEY,
    dept_name VARCHAR2(50),
    location VARCHAR2(50),
    budget NUMBER(12,2)
);

CREATE TABLE only_in_db1 (
    id NUMBER(10) PRIMARY KEY,
    description VARCHAR2(100)
);

-- Table with column difference
CREATE TABLE diff_table (
    id NUMBER(10) PRIMARY KEY,
    col1 VARCHAR2(20),
    col2 NUMBER(5,2),
    col3 DATE
);
