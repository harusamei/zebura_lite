import pandas as pd
import types
tables = [{'table_name': 'table1','count':1}, {'table_name': 'table2','count':2}, {'table_name': 'table1','count':3}]
tlist = [table['table_name'] for table in tables]
print(tlist)
        # 步长-1, 反转，找到最后一个sql4db的位置
reverse_indx = tlist[::-1].index('table2')
print(reverse_indx)
indx = len(tlist) - reverse_indx - 1
print(indx)
print(tables[indx])