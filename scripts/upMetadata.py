# 上传人工check过的metadata.xlsx到系统数据库
import sys,os
sys.path.insert(0, os.getcwd().lower())
from dbaccess.meta_update import metaIntoAdmdb
import argparse

if __name__ == "__main__":  
    parser = argparse.ArgumentParser(description='upload/update metadata.xls to system database')
    parser.add_argument('--csv_path', type=str, required=True, help='Path of metadata.xlsx')
    args = parser.parse_args()
    xls_name = args.csv_path
    xls_name = os.path.join(os.getcwd(), xls_name)
    print(f"Uploading metadata: {xls_name}")
    metaIntoAdmdb(xls_name)
    print("Done")
