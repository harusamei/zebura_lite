# 优化CSV数据,包括删除全为空的行，删除重复列，规范列名，规范数据类型，填充空值
# 生成存入数据库的表头信息，包括数据类型，是否为空，默认值，主键等
#############################
import pandas as pd
from dateutil.parser import parse
import os
import re
import json


class optz_data:
    # data type mapping, pandas to mysql
    dtype_mapping = {
        'int64': 'BIGINT',
        'int32': 'INT',
        'float64': 'DOUBLE',
        'float32': 'FLOAT',
        'bool': 'TINYINT(1)',
        'object': 'TEXT',
        'string': 'VARCHAR(255)',
        'datetime64[ns]': 'DATETIME',
        'category': 'VARCHAR(255)'
    }
    # 不同database的value类型映射, value type的等价关系
    vtype_eqs = None

    def __init__(self):
        if optz_data.vtype_eqs is None:
            cwd = os.getcwd()
            file_path = os.path.join(cwd, 'dbaccess/vtype_eqs.json')
            try:
                with open(file_path, 'r') as f:
                    optz_data.vtype_eqs = json.load(f)
            except Exception as e:
                print(f"Error: {e}")
                optz_data.vtype_eqs = {}
        return
           
    # 单纯优化表格，与数据库无关
    def optz_csv(self,df):
        if df is None or df.empty:
            return df
        # 删除全为空的行
        df.dropna(axis=0, how='all', inplace=True)
        col_names1 = df.columns.tolist()
        # 列名不使用空格，括号或特殊字符，列名变为小写
        df = self.regz_names(df)
        # 删除重复列
        df = self.dedup_cols(df)
        col_names2 = df.columns.tolist()
        set1 = set(col_names1)-set(col_names2)
        set2 = set(col_names2)-set(col_names1)
        if set1:
            print(f"Columns removed: {set1}")
        if set2:
            print(f"Columns changed to: {set2}")
        # 规范/识别数据类型
        df = self.regz_dtypes(df)
        return df

    # 得到表的数据类型
    # headers={{column_name:{dtype},{}...}, 以column_name为key
    def get_headers_dtypes(self,df):
        df = df.convert_dtypes()
        tDict = df.dtypes.to_dict()
        headers = {c_name: {'dtype': c_type.name} for c_name, c_type in tDict.items()}
        return headers
    
    # 确定每列对应数据库的数据类型
    # database_type: mysql, postgres...
    # headers={'col1': {'vtype': 'BIGINT', 'default': 0}, ...}
    # database_type 为None时，不做数据类型转换
    def get_db_fields(self, df, database_type='mysql'):

        if df is None or df.empty:
            print("Error: empty dataframe")
            return None
        
        database_type = database_type.lower()
        if database_type not in optz_data.vtype_eqs['_databases']:
            print(f"Error: {database_type} not supported")
            return None

        df = df.convert_dtypes()
        # 得到column name, dtype
        tDict = df.dtypes.to_dict()
        fields = {}
        for c_name, c_type in tDict.items():
            # df的数据类型转换为mysql数据库数据类型
            ctype_str=c_type.name.lower()
            val_type = self.dtype_mapping.get(ctype_str)
            if val_type is None:
                print(f"Error: no database mapping of {ctype_str}")
                continue
            # 类似VARCHAR(255)的数据类型，分为词干和后缀
            stem = val_type.split('(')[0]
            suffix = val_type.split('(')[1] if '(' in val_type else ''
            if stem in self.vtype_eqs:
                # 切换为当前需要的database类型，如mysql, postgres
                val_type = self.vtype_eqs[stem][database_type]
                val_type += f'({suffix}' if suffix else ''
                fields[c_name] = {'vtype': val_type}
                fields[c_name]['default'] = self.vtype_eqs[stem]['default']
        return fields
    
    # 根据fields对df的一些行的值进行规范化，以便导入database
    # fields={'col1': {'vtype': 'BIGINT', 'default': 0}, ...}
    # 输出 [tuple], 每个tuple为一行数据
    def regz_values(self,rows, fields):
        tuples = []
        col_names = rows.columns.tolist()
        for i in range(0, len(rows)):
            row = rows.iloc[i].copy()
            for c_name, c_dict in fields.items():
                if c_name not in col_names:
                    continue
                v_type = c_dict.get('vtype')
                default = c_dict.get('default')
                if v_type is None:
                    continue
                if v_type.startswith('VARCHAR'):
                    row.loc[c_name] = str(row[c_name])[:int(v_type.split('(')[1][:-1])]
                elif v_type.startswith('INT') or v_type.startswith('FLOAT') or v_type.startswith('DOUBLE'):
                    row.loc[c_name] = re.sub(r'[^0-9.]', '', str(row[c_name]))
                elif v_type.startswith('BOOL'):
                    row.loc[c_name] = bool(row[c_name])
                elif v_type.startswith('DATE'):
                    row.loc[c_name] = self.normalize_datetime(row[c_name])
                if pd.isna(row[c_name]) or len(str(row[c_name])) == 0:
                    row.loc[c_name] = default
                
                if 'INT' in v_type and row[c_name] is not None:
                    row.loc[c_name] = int(row[c_name])
                elif ('FLOAT' in v_type or 'DOUBLE' in v_type) and row[c_name] is not None:
                    row.loc[c_name] = float(row[c_name])

            tuples.append(tuple(row))   
        return tuples

    # 整形，列名不使用空格和特殊字符，虽然列名可以使用空格，但它会让查询变得繁琐
    @staticmethod
    def regz_names(df):
        for c_name in df.columns:       #列名不使用空格和特殊字符
            name = c_name
            name = re.sub(r'[()\[\]{}]', ' ', name)
            name = re.sub(r'[^\w\s]', '', name)  # 移除所有非字母数字字符
            name = name.replace(' ', '_')  # 用下划线替换空格
            name = name.strip('_') # 去掉首尾下划线
            name = name.lower()
            if name != c_name:
                df = df.rename(columns={c_name: name})
                print(f"Column {c_name} renamed to {name}")
        return df

    # 规范表格的数据类型，与数据库无关
    def regz_dtypes(self,df):
        # 将数据类型转换为最适合的类型
        df = df.convert_dtypes()
        # 得到column name, dtype
        tDict = df.dtypes.to_dict()
        for c_name, c_type in tDict.items():
            if c_type.name not in ['object','string']:
                tDict[c_name] = c_type.name
                continue
            # 采样数据，推断数据类型
            samples = df[c_name].dropna().unique()[:100]
            c_type = self.infer_dtype(samples)
            tDict[c_name] = c_type
        try:
            for c_name, c_type in tDict.items():
                if 'date' in c_type:
                    df[c_name] = df[c_name].apply(lambda x: self.normalize_datetime(x) if pd.notnull(x) else x)
            df = df.astype(tDict)
        except Exception as e:
            print(f"Error: {e}")
        return df
       
    
    # CSV的列名可能有重复，删除重复列
    @staticmethod
    def dedup_cols(df):
        dupIndx = df.columns.duplicated()
        df = df.loc[:, ~dupIndx]
        return df
    
    def infer_dtype(self, samples):
        # 从数据样本推断数据类型
        dtype_hits = {}
        for sample in samples:
            if sample is None:
                continue
            if self.is_number(sample):
                if '.' in str(sample):
                    dtype = 'float64'
                else:
                    dtype = 'int64'
            elif self.is_date(sample):
                dtype = 'datetime64[ns]'
            elif sample.lower() in ['true', 'false']:
                dtype = 'bool'
            elif len(str(sample)) > 255:
                dtype = 'object'
            else:
                dtype = 'string'
            dtype_hits[dtype] = dtype_hits.get(dtype, 0) + 1
        
        if not dtype_hits:  # 无数据
            return 'string'
        return max(dtype_hits, key=dtype_hits.get)
    
    @staticmethod
    def is_date(date_str):
        try:
            parse(date_str)
            return True
        except ValueError:
            return False
    
    @staticmethod
    def is_number(s):
        try:
            float(s)  # 尝试转换为浮点数
            return True
        except ValueError:
            return False
    
    # 日期时间格式规范化
    @staticmethod
    def normalize_datetime(date_str):
        fmt="%Y-%m-%d %H:%M:%S"
        try:
            if not isinstance(date_str, str):
                date_str = str(date_str)
            dt = parse(date_str)        # 解析时间
            return dt.strftime(fmt)     # 规范格式
        except ValueError:
            return pd.NaT               # 解析失败返回 NaT

if __name__ == '__main__':
    df_optz = optz_data()
    data = {'value(int)': [1, 2.1, pd.NA, 4], 
            'name:': ['a','b','','d'],
            'name': ['a','b','c','d'], 
            'date': ['2021-01-01', '2021年1月4日', 'January 4, 2021', '2021-01-04']}
    df = pd.DataFrame(data)
    df = df_optz.optz_csv(df)
    print(df_optz.get_headers_dtypes(df))
    fields = df_optz.get_db_fields(df, 'postgres')
    rows = df[0:4]
    tuples = df_optz.regz_values(rows,fields)
    print(fields)
    print(tuples)
