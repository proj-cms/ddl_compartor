# Oracle DDL Comparator

This project compares table/view columns between two Oracle databases and generates an Excel report with:
- **DiffColumns**: Columns with same name but different precision, scale, nullability, etc.
- **OnlyInDB1**: Columns only in first database.
- **OnlyInDB2**: Columns only in second database.

## Usage
1. Update `src/config.yaml` with your Oracle DB connection details.
   - You can set `retry_count` and `retry_delay` in each DB config for connection retries.
2. Run the pipeline:
   ```bash
   python src/db_compare.py
   ```
3. Output Excel file `ddl_compare_result.xlsx` will be generated.

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