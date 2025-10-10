
import unittest
import pandas as pd
import os
import openpyxl
from src.excel.excel_writer import ExcelWriter

class TestExcelWriter(unittest.TestCase):

    def setUp(self):
        self.test_file = 'test_result.xlsx'

    def tearDown(self):
        if os.path.exists(self.test_file):
            os.remove(self.test_file)

    def test_write_excel(self):
        # Create sample dataframes
        diff = pd.DataFrame({'col1': [1, 2], 'col2': ['A', 'B']})
        only_db1 = pd.DataFrame({'col3': [3, 4], 'col4': ['C', 'D']})
        only_db2 = pd.DataFrame({'col5': [5, 6], 'col6': ['E', 'F']})

        # Write to excel
        ExcelWriter.write(diff, only_db1, only_db2, self.test_file)

        # Check if file exists
        self.assertTrue(os.path.exists(self.test_file))

        # Check content of the excel file
        wb = openpyxl.load_workbook(self.test_file)

        # Check sheet names
        self.assertEqual(wb.sheetnames, ['DiffColumns', 'OnlyInDB1', 'OnlyInDB2'])

        # Check DiffColumns sheet
        ws_diff = wb['DiffColumns']
        self.assertEqual(ws_diff['A1'].value, 'col1')
        self.assertEqual(ws_diff['B1'].value, 'col2')
        self.assertEqual(ws_diff['A2'].value, 1)
        self.assertEqual(ws_diff['B3'].value, 'B')

        # Check OnlyInDB1 sheet
        ws_only_db1 = wb['OnlyInDB1']
        self.assertEqual(ws_only_db1['A1'].value, 'col3')
        self.assertEqual(ws_only_db1['B1'].value, 'col4')
        self.assertEqual(ws_only_db1['A2'].value, 3)
        self.assertEqual(ws_only_db1['B3'].value, 'D')

        # Check OnlyInDB2 sheet
        ws_only_db2 = wb['OnlyInDB2']
        self.assertEqual(ws_only_db2['A1'].value, 'col5')
        self.assertEqual(ws_only_db2['B1'].value, 'col6')
        self.assertEqual(ws_only_db2['A2'].value, 5)
        self.assertEqual(ws_only_db2['B3'].value, 'F')

if __name__ == '__main__':
    unittest.main()
