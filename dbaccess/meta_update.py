# metadata.xlsx， 即表的元数据, 存放在admdb中admdata db下
# 该表可以用于prompt, 以及用户对数据库本身的提问，比如我有哪些数据表，每个表有哪些字段等
# 在 zebura_lit 不适用
#########################################

import sys,os
sys.path.insert(0, os.getcwd().lower())
from settings import z_config
import zebura_core.constants as const
from zebura_core.placeholder import make_dbServer
from discard.conndb import connect
from dbaccess.csv2sql1 import CSV2SQL
import pandas as pd

def read_metatb(pj_name, chat_lang):

    adm_serName = const.C_ADM_dbServer
    dbServer = make_dbServer(adm_serName)
    db_name = z_config[adm_serName,'db_name']
    dbServer['db_name'] = db_name.lower()
    cnx = connect(dbServer)
    cursor = cnx.cursor()
    tb_name = const.Z_META_TBNAME.format(pj_name=pj_name)
    query = f"""SELECT * FROM {tb_name}
            where LOWER(chat_lang)=LOWER('{chat_lang}')
            """
    cursor.execute(query)
    rows = cursor.fetchall()
    if len(rows) < 1:
        print(f"No {chat_lang} metadata found for {tb_name}, replacing with English metadata.")
        query = f"""SELECT * FROM {tb_name}
            where LOWER(chat_lang)=LOWER('English')
            """
        cursor.execute(query)
        rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]  # 获取列名
    df = pd.DataFrame(rows,columns=columns)
    return df

# update system database with metadata.xlsx
def metaIntoAdmdb(xls_name):

    df = pd.read_excel(xls_name, sheet_name=None)
    if 'database' not in df or 'tables' not in df or 'fields' not in df:
        print(f"format error: {xls_name}")
        return False
    
    pdf = df['database']
    pj_name = pdf['database_name'][0]
    chat_lang = pdf['chat_lang'][0]
    tb_name = const.Z_META_TBNAME.format(pj_name=pj_name)

    adm_serName = const.C_ADM_dbServer
    dbServer = make_dbServer(adm_serName)
    db_name = z_config[adm_serName,'db_name']
    db_type = dbServer['db_type']
    dbServer['db_name'] = db_name.lower()

    if db_type not in ['mysql','postgres']:
        print(f"Unsupported database type: {db_type}")
        return False
    csv2sql = CSV2SQL(dbServer)
    
    merged_df = pd.merge(df['tables'], df['fields'], on='table_name') 
    firstRow = pdf.iloc[[0]]
    firstRow = firstRow.loc[firstRow.index.repeat(len(merged_df))]
    # 合成一张大宽表
    merged_df = pd.concat([merged_df, firstRow.reset_index(drop=True)], axis=1)
    
    csv2sql.drop_dupCols(merged_df)
    csv2sql.regularize_df(merged_df)

    tb_name = csv2sql.create_table(merged_df, tb_name, drop=False)
    if tb_name is None:
        print(f"Failed to create table {tb_name}")
        return False
    
    columns = csv2sql.ops.show_columns(tb_name)
    if set(merged_df.columns) != set(columns):
        print(f"Columns not match: {set(merged_df.columns) - set(columns)}")
        return False
    
    cnx = csv2sql.ops.cnx
    cursor = cnx.cursor()
    query = f"""DELETE FROM {tb_name} 
                where LOWER(chat_lang)=LOWER('{chat_lang}')
                """
    cursor.execute(query)

    count = csv2sql.ops.count_items(tb_name)
    print(f'count of items is {count} after delecting previous data')
    
    csv2sql.saveInDB(merged_df, tb_name)
    count = csv2sql.ops.count_items(tb_name)
    print(f'count of items is {count} after saving new data')
    return True

if __name__ == '__main__':
    xls_name = 'C:/something/talq/zebura_db/training/IMDB/metadata.xlsx'
    metaIntoAdmdb(xls_name)
    # df = read_metatb('IMDB', 'English')
    # print(df)
    # print("Done")
