import pandas as pd
import logging

import pandas as pd
from typing import Any

class ExcelWriter:
    """A class to write the comparison results to an Excel file."""
    @staticmethod
    def write(diff: pd.DataFrame, only_db1: pd.DataFrame, only_db2: pd.DataFrame, out_path: str) -> None:
        """Writes the comparison results to an Excel file.

        Args:
            diff (pd.DataFrame): A DataFrame containing the differences between the two databases.
            only_db1 (pd.DataFrame): A DataFrame containing the tables only in the first database.
            only_db2 (pd.DataFrame): A DataFrame containing the tables only in the second database.
            out_path (str): The path to the output Excel file.
        """
        import openpyxl
        from openpyxl.styles import PatternFill
        logger = logging.getLogger(__name__)
        try:
            logger.info(f"Writing results to Excel file: {out_path}")
            with pd.ExcelWriter(out_path, engine='openpyxl') as writer:
                diff.to_excel(writer, sheet_name='DiffColumns', index=False)
                only_db1.to_excel(writer, sheet_name='OnlyInDB1', index=False)
                only_db2.to_excel(writer, sheet_name='OnlyInDB2', index=False)
            # Highlight differences in DiffColumns sheet
            wb = openpyxl.load_workbook(out_path)
            ws = wb['DiffColumns']
            # Find columns that differ and highlight them
            header = [cell.value for cell in ws[1]]
            diff_cols = [i for i, col in enumerate(header) if col.endswith('_db1') or col.endswith('_db2')]
            fill = PatternFill(start_color='FFFF00', end_color='FFFF00', fill_type='solid')
            for row in ws.iter_rows(min_row=2, max_row=ws.max_row):
                for i in diff_cols:
                    if row[i].value is not None:
                        row[i].fill = fill
            wb.save(out_path)
            logger.info("Excel file written and differences highlighted successfully.")
        except Exception as e:
            logger.error(f"Error writing Excel file: {e}")
            raise
