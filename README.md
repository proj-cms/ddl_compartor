# Oracle DDL Comparator

This project compares table/view columns between two Oracle databases and generates an Excel report with:
- **DiffColumns**: Columns with same name but different precision, scale, nullability, etc.
- **OnlyInDB1**: Columns only in primary (reference) database.
- **OnlyInDB2**: Columns only in secondary (target) database.

## Usage
1. Update `src/config.yaml` with your Oracle DB connection details.
   - Set `primary_db` to `oracle_db1` or `oracle_db2` to specify which database is the primary source of comparison (defaults to `oracle_db1`).
   - You can set `retry_count` and `retry_delay` in each DB config for connection retries.
2. Run the pipeline:
   ```bash
   python src/db_compare.py
   ```
3. Output Excel file `ddl_compare_result.xlsx` will be generated.

## Configuration

### Primary Database Selection
You can designate one database as the primary source of comparison by setting the `primary_db` field in `config.yaml`:

```yaml
# Primary database to use as source of comparison (oracle_db1 or oracle_db2)
primary_db: oracle_db1  # or oracle_db2

oracle_db1:
  host: localhost
  port: 1521
  service_name: XE
  username: test_schema
  password: test1234
  retry_count: 3
  retry_delay: 5

oracle_db2:
  host: localhost
  port: 1522
  service_name: XE
  username: test_schema
  password: test1234
  retry_count: 3
  retry_delay: 5
```

**Benefits:**
- The primary database is always used as the reference (DB1 in comparison results)
- Clear designation of "source of truth" in logs (PRIMARY DB vs SECONDARY DB)
- Flexible comparison direction without changing database order in config
- Defaults to `oracle_db1` if not specified or invalid

## Dev Testing
- Use Docker to bring up two Oracle DB pods for local testing.
- Example docker-compose is provided in `docker-compose.yml`.

## CI/CD
- GitHub Actions workflow is provided in `.github/workflows/ci.yml` for build and test automation.

## Unit Tests
- Run tests with:
   ```bash
   python -m unittest discover tests
   ```

## Logging & Error Handling
- All modules use detailed logging for diagnostics.
- Errors are logged and handled gracefully; critical failures abort the pipeline.
- DB connection uses retry logic (configurable in `config.yaml`).

## Project Folder Structure
```
ddl_comparator/
├── .gitignore
├── docker-compose.yml
├── README.md
├── requirements.txt
├── .git/
├── .github/
│   └── workflows/
│       └── ci.yml
├── .venv/
├── src/
│   ├── __init__.py
│   ├── config.yaml
│   ├── db_compare.py
│   ├── docker_commands
│   ├── db/
│   │   ├── db1_admin.sql
│   │   ├── db1_sample_tables.sql
│   │   ├── db2_admin.sql
│   │   ├── db2_sample_tables.sql
│   │   └── oracle_db.py
│   └── excel/
│       └── excel_writer.py
└── tests/
    ├── __init__.py
    ├── test_comparator.py
    ├── test_db_compare.py
    └── test_excel_writer.py
```