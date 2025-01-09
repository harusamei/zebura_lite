# 处理excel文件的工具类
#######################
import pandas as pd

def read_excel_sheets(file_path):
    # Read the Excel file
    xls = pd.ExcelFile(file_path)
    # Get the sheet names
    sheet_names = xls.sheet_names

    # Iterate over each sheet
    for sheet_name in sheet_names:
        # Read the sheet into a DataFrame
        df = pd.read_excel(file_path, sheet_name=sheet_name)
        num_rows = df.shape[0]
        print(f"{sheet_name}, num of rows is {num_rows}")
        # Get the columns
        print(df.columns.tolist())
        print(df.loc[0,'table_name'])


# Example usage
if __name__ == '__main__':
    file_path = "C:\something\zebura\\training\it\dbInfo\metadata.xlsx"
    sheets_data = read_excel_sheets(file_path)
