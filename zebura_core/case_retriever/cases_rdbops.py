#####################
# admdb 插入删除查询 cases
#####################
import os,sys, time
sys.path.insert(0, os.getcwd())
from settings import z_config
import zebura_core.constants as const
from dbaccess.csv2sql import CSV2SQL
from zebura_core.placeholder import make_dbServer
from zebura_core.utils.hashID_maker import string2id
import pandas as pd

# 将生成的cases存入adm数据库 
class Case_updater:

    def __init__(self):

        adm_serName = const.C_ADM_dbServer
        dbServer = make_dbServer(adm_serName)
        adm_dbName = z_config[adm_serName,'db_name']
        dbServer['db_name'] = adm_dbName.lower()

        self.dbServer = dbServer
        self.csv2sql = CSV2SQL(dbServer)
        print(f"Case_updater init success")

    # 将cases存入adm数据库
    def insert_into_Admdb(self, csv_name):
        dtypes = const.Z_CASES_FIELDS.copy()
        dateCols = [colName for colName, colType in dtypes.items() if colType == 'date']
        for col in dateCols:    # 日期列不能解析，转换为字符串
            dtypes[col] = 'str'
        df = pd.read_csv(csv_name)
        # Drop empty rows and columns
        df.dropna(how='all', inplace=True)          # Drop rows where all elements are NA
        df.dropna(axis=1, how='all', inplace=True)  # Drop columns where all elements are NA
        self.csv2sql.astype_df(df, dtypes)          # Convert columns to specified data types

        pj_name = df['database_name'][0]
        # 一个database对应一个project,所有表生成的cases都放在一个 cases_{pj_name}表中
        tb_name = const.Z_CASES_TBNAME.format(pj_name=pj_name)

        self.csv2sql.drop_dupCols(df)
        self.csv2sql.regularize_df(df)
        # 增加一列 'deleted' 赋值为 False
        df['deleted'] = 'FALSE'
        
        tb_name = self.csv2sql.create_table(df, tb_name, drop=True, 
                                            dtypes=const.Z_CASES_FIELDS, pk='id')
        if tb_name is None:
            print(f"Failed to create table {tb_name}")
            return False
        ops = self.csv2sql.ops
        columns = ops.show_columns(tb_name)
        if set(df.columns) != set(columns):
            print(f"Columns not match: {set(df.columns) - set(columns)}")
            return False
        
        sxm_df = ops.get_tb_schema(tb_name)
        headers = const.Z_CASES_FIELDS.keys()
        newAdd = 0
        for _, row in df.iterrows():
            row = ops.refine_data(sxm_df, row)
            addnum = self.insert_row(tb_name, headers, row)
            if addnum is not None:
                newAdd += addnum
        
        count = ops.count_items(tb_name)
        print(f'count of new items is {count}')
        return
    
    # return None, 1, 0
    def insert_row(self, tb_name, headers, row):
        if row is None:
            print("Data type mismatch, skip this row")
            return None
        timeStr = time.strftime("%Y-%m-%d", time.localtime())
        insert_query = self.csv2sql.ops.get_insert_query(tb_name,headers)
        select_query = f"SELECT id FROM {tb_name} WHERE id=%s LIMIT 1"
        update_query = f"""UPDATE {tb_name}
                        SET hit = hit + 1, updated_date = '{timeStr}', deleted =FALSE
                        WHERE id=%s"""
        
        try:
            cnx = self.csv2sql.cnx
            cursor = cnx.cursor()
            cursor.execute(select_query, (row['id'],))
            result = cursor.fetchall()
            if result:
                print(f"Question '{row['question']}' already exists in the table.")
                key_id = result[0][0]
                cursor.execute(update_query, (key_id,))
                cnx.commit()
                return 0
            else:
                values = tuple(row.values())
                cursor.execute(insert_query, values)
                cnx.commit()
                return 1
        except Exception as e:
            print(f"insert_row in {tb_name} Error: {e}")

        return None
    
    # 删除表，只必须包含question和table_name两列,其它列不需要
    def del_from_Admdb(self, pj_name, csv_name):
        df = pd.read_csv(csv_name)
        df['id'] = df.apply(lambda row: string2id(f"{row['question']} in {row['table_name']}"), axis=1)

        tb_name = const.Z_CASES_TBNAME.format(pj_name=pj_name)
        del_count = 0
        for _, row in df.iterrows():
            key_id = row['id']
            delnum = self.del_row(tb_name, key_id)
            if delnum:
                del_count += delnum
        print(f'count of deleted items is {del_count}')
        return
    
    def del_row(self, tb_name, key_id):
        timeStr = time.strftime("%Y-%m-%d", time.localtime())
        set_query = f"UPDATE {tb_name} SET deleted = TRUE, updated_date = '{timeStr}'  WHERE id=%s"
        try:
            cnx = self.csv2sql.cnx
            cursor = cnx.cursor()
            cursor.execute(set_query, (key_id,))
            cnx.commit()
            return 1
        except Exception as e:
            print(f"Delete {tb_name} Error: {e}")
        return 0
    
    # 不必经常使用，且与ES同步前不要使用
    def hard_delete(self, pj_name):
        tb_name = const.Z_CASES_TBNAME.format(pj_name=pj_name)
        delete_query = f"DELETE FROM {tb_name} WHERE deleted = TRUE"
        try:
            cnx = self.csv2sql.cnx
            cursor = cnx.cursor()
            cursor.execute(delete_query)
            cnx.commit()
            print(f'deleted from {tb_name}')
        except Exception as e:
            print(f"Delete {tb_name} Error: {e}")


# Example usage
if __name__ == '__main__':
    saver = Case_updater()
    saver.insert_into_Admdb('training/cases.csv')
    #saver.del_from_Admdb(pj_name='imdb',csv_name='training/del_cases.csv')
