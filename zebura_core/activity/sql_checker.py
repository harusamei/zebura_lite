# 通过查DB， check SQL 错误，包括表名，列名，值
#######################################
import sys,os
sys.path.insert(0, os.getcwd().lower())
import zebura_core.constants as const
from zebura_core.nltosql.schlinker import Sch_linking
from zebura_core.knowledges.schema_loader_lite import ScmaLoader
from zebura_core.utils.sqlparser1 import ParseSQL
import logging, re, random, itertools,asyncio
from typing import Union, Dict, Any
from zebura_core.utils.conndb import connect


class CheckSQL:

    def __init__(self, dbServer, chat_lang='english'):
        self.db_name = dbServer['db_name']
        self.cnx = connect(dbServer)
        if self.cnx is None:
            raise ValueError("Database connection failed")
        
        self.scha_loader = ScmaLoader(self.db_name, chat_lang)
        self.sl = Sch_linking(const.D_SIMILITY_THRESHOLD)
        self.ps = ParseSQL()
        logging.info("CheckSQL init done")
        
    # 主功能, 值只检查=, like，不进行值的扩展
    # 将错误信息和建议都放在msg列表中
    async def check_sql(self, sql:Union[str,Dict[str, Any]]) -> dict:
        all_checks = self.make_checkDict('succ')
        # 解析SQL，获取check points
        if isinstance(sql,str):
            slots = await self.ps.extract_sql(sql)
        elif isinstance(sql, dict) and 'sql' in sql:
            slots = sql
            slots['status'] = 'succ'
        else:
            slots['msg'] = 'Error: invalid breakdown of sql'
            slots['status'] = 'failed'
            
        if slots['status'] == 'failed':
            all_checks['msg'].append(slots['msg'])
            all_checks['status'] = 'failed'
            return all_checks
       
        ckps = self.get_checkPoints(slots)
        if ckps['status'] == 'failed':
            all_checks['msg'].append(ckps['msg'])
            all_checks['status'] = 'failed'
        else:
            all_checks = self.check_sql_with_db(sql, ckps)
        
        return all_checks
    
    # 静态检查，不连接数据库, 检查表名，列名
    # sql it self or dict {sql:'', tables:[], columns:[], values:[]}
    async def check_syntax(self, sql:Union[str,dict[str,Any]]) -> dict:

        all_checks = self.make_checkDict('succ')
        if isinstance(sql, str):
            # 解析SQL，获取check points
            slots = await self.ps.extract_sql(sql)
        elif isinstance(sql, dict) and 'tables' in sql:
            slots = sql
            slots['status'] = 'succ'
        else:
            slots['msg']='Error: invalid breakdown of sql'
            slots['status'] = 'failed'
        
        if slots['status'] == 'failed':
            all_checks['msg'].append(slots['msg'])
            all_checks['status'] = 'failed'
            return all_checks
        
        tb_names = [ele.get('name','') for ele in slots['tables']]
        matched = self.sl.link_tables(tb_names)
        # name, matched_name
        for tup in matched:
            all_checks['tables'][tup[0]]=tup[1]   
        
        col_names = [ele.get('name','') for ele in slots['columns'] ]
        matched = self.sl.link_fields(col_names)
        #col_name, matched_name,tabel_name
        for tup in matched:
            all_checks['columns'][tup[0]] =(tup[1],tup[2])

        return all_checks
    
    # 通过运行检查SQL, sql 为原始SQL, CKPS 为解析后的check points
    def check_sql_with_db(self,sql, ckps) -> dict:

        tmp11 = "Error: Table '{table_name}' doesn't exist"                 # 表不存在
        tmp12 = "Error: '{col_name}' not found in field list" # 列不存在
        tmp13 = "Warning: value '{val}' was not found in the column '{col}'"     # 值不存在
        tmp14 = 'Error: error in your SQL syntax'                   # 语法错误
        
        tmp21 = "Suggestion: The table name most similar to '{table_name1}' in the database is '{table_name2}'"    # 相似表名
        tmp22 = "Suggestion: The column name most similar to '{col_name1}' in the field list is '{col_name2}'"    # 相似列名
                                                                                                                # 值语义扩展
        tmp23 = "Suggestion: Values semantically similar to '{val1}', such as '{val2}', can be found in the '{col}' using fuzzy matching"
        tmp24 = "Suggestion: '{val}' can be found in '{col}' using SQL's fuzzy matching"                            # 值模糊匹配

        all_checks = self.make_checkDict('succ')
        # ckps = {'status':'succ','tables': [], 'columns': [], 'conds': []}
        # "tables": [ {"name": "employees", "alias": "t1"}]
        # 所有涉及的表名 mapping
        if len(ckps['tables']) < 1:
            all_checks['msg'].append('Error: No table found in the SQL')
            all_checks['status'] = 'failed'
            return all_checks
        
        table_names = [ele.get('name','') for ele in ckps['tables']]
        # 理想状态，完全匹配
        table_check = {'status': 'succ'}
        matched = self.sl.link_tables(table_names)
        for tup in matched:
            table_check[tup[0]] = tup[1]
            # 与原SQL不一致都算failed
            if tup[0] != tup[1]:
                table_check['status'] = 'failed'
                all_checks['msg'].append(tmp11.format(table_name=tup[0]))
                if tup[1] is not None:
                    all_checks['msg'].append(tmp21.format(table_name1=tup[0], table_name2=tup[1]))
        
        # 列名 mapping， table_name 可能为 none
        # "fields": [{"name": "name", "table": "employees", "alias": "employee_name"}]
        fields = [ele.get('name','') for ele in ckps['columns'] ]
        tbs = [ele.get('table','') for ele in ckps['columns']]
        fields_check = {'status': 'succ'}
        # (ori_term, col_like, tb_like, score)
        matched = self.sl.link_fields(fields)                   # 暂时未做表名限制，column names在所有表中匹配  
        for tup in matched:
            if tup[0] == tup[1] and tup[0] in fields:                 # 完全匹配, 保留原始所属表
                loc = fields.index(tup[0])
                t_tb = tbs[loc]
                fields_check[tup[0]] = (tup[1],t_tb)
            else:
                fields_check['status'] = 'failed'           # 不完全一致则failed
                all_checks['msg'].append(tmp12.format(col_name=tup[0]))
                if tup[1] is not None:
                    all_checks['msg'].append(tmp22.format(col_name1=tup[0], col_name2=tup[1]))
                    fields_check[tup[0]] = (tup[1],tup[2])  # 相似列名可能存在于多个表中
            
        # 值检查
        values_check = {'status':'succ'}
        for item in ckps['values']:
            tb = item.get('table','')
            field = item.get('column','')
            val = item.get('value','')
            # [True, val, 'EXCT']
            tup = self.check_value(tb, field, val)
            values_check[f'{field},{val}'] = tup
            # 4种情况,true, fuzzy, exct, emty
            if tup[0] is False:
                values_check['status'] = 'failed'
            if tup[0] is False:
                all_checks['msg'].append(tmp14)                     # SQL 执行错误
            elif tup[2] in ['FUZZY']:                        # 不完全一致则failed
                all_checks['msg'].append(tmp13.format(val=val, col=field)) # 值不存在
                if tup[2] == 'FUZZY':
                    all_checks['msg'].append(tmp24.format(val1=val, col=field))
            elif tup[2] in ['EMTY']:
                all_checks['msg'].append(tmp13.format(val=val, col=field)) # 值不存在
                 
        all_checks['values'] = values_check
        all_checks['tables'] = table_check
        all_checks['columns'] = fields_check

        tup = self.execute_sql(sql)                 # 执行整个SQL
        if tup[0] is False:
            all_checks['status'] = 'failed'
            all_checks['msg'].append(tup[1])
        else:
            all_checks['msg'].append('correct SQL')

        return all_checks

    def get_checkPoints(self,slots) -> dict:
        ckps = {'status':'succ','tables': [], 'columns': [], 'values': []}
        if (not isinstance(slots,dict)) or slots['status'] != 'succ': 
            ckps['status'] = 'failed'
            ckps['msg'] = 'Error: invalid breakdown of sql'
            return ckps
        
        # "tables": [ {"name": "employees", "alias": "t1"}]
        # "columns": [{"name": "name", "table": "employees", "alias": "employee_name"}]
        # "values": [{"col": "name", "val": "John","table": "employees"}]
        
        ckps['tables']= slots['tables']
        ckps['columns'] = []
        not_allowed = ['COUNT','SUM','AVG','MAX','MIN','*']
        for field in slots['columns']:
            col_name = field.get('name')
            if col_name in not_allowed:
                continue
            ckps['columns'].append(field)
         
        # 只检查字符型的值 =, like
        ckps['values'] = []
        for value in slots['values']:
            tval = value.get('value')
            if tval is not None and isinstance(tval,str):
                ckps['values'].append(value)
        return ckps

    # 根据check结果，生成 relevant tables
    def gen_rel_tables(self, all_checks):
        tb_names = []
        checks = all_checks['tables']
        # key: table_name, value: matched_name
        for _, tb1 in checks.items():
            if tb1 is not None:
                tb_names.append(tb1)
        checks = all_checks['columns']
        base = ['']
        # key: col, value: col_like, table
        # 每个column 所属表的全组合
        for _, (col1, tbs) in checks.items():
            if tbs is not None:
                list1 = tbs.split(';')
                base = list(itertools.product(base, list1))
                base = [f'{x[0]};{x[1]}'.strip(';') for x in base]
        if base == ['']:
            base = [set(tb_names)]
        else:
            base = [set(x.split(';')).union(set(tb_names))for x in base]
            base = sorted(base, key=lambda x: len(x))
        return base[0]
    
    # True 正确查询， False 错误查询， val 引号已经去掉
    # False, 'errmsg',''; True, val, 'EXCT' 完全匹配; True, val, 'FUZZY' 模糊匹配; True, val, 'EMTY' 模糊查询无值
    def is_value_exist(self, table_name, col, val, ttype='varchar'):

        quy1 = "SELECT {col} FROM {table_name} WHERE {col} = '{val}' LIMIT 1"
        quy2 = "SELECT {col} FROM {table_name} WHERE {col} LIKE '%{val}%' LIMIT 1"
        query1 = None
        query2 = None
        ttype = ttype.partition('(')[0]
        if '%' not in val:
            query1 = quy1.format(col=col, table_name=table_name, val=val)
        if ttype.lower() in ['varchar', 'char', 'text']:                # 只字符串类型FUZZY 查询
            val = val.replace('%', '')
            query2 = quy2.format(col=col, table_name=table_name, val=val)
        
        check = [True, val, 'EXCT'] # 默认结果
        # 完全匹配
        if query1 is not None:
            tup = self.execute_sql(query1)
            if tup[0] is False:
                return tup
            elif tup[1] > 0:
                return tuple(check)                     # (True, val, 'EXCT')              
        # 模糊匹配
        check = [True, f'%{val}%', 'FUZZY'] # 默认结果
        if query2 is not None:
            tup = self.execute_sql(query2)
            if tup[0] is False:
                return tup                              # (False, 'errmsg','')
            elif tup[1] > 0:
                return tuple(check)                     # (True, val, 'FUZZY')  
                   
        return [True, val, 'EMTY']                      # (True, val, 'EMTY')

    # 执行SQL
    def execute_sql(self, sql) -> tuple:
        cursor = self.cnx.cursor()
        try:
            cursor.execute(sql)
            result = cursor.fetchall()
            return (True, len(result), result)
        except Exception as e:
            return (False, f'ERROR: {e}','')
        
    # 字符类value， 可能需要模糊和查询扩展
    # 返回值：(True, val, 'FUZZY') 有值，[True, val, 'EXCT'] excetly 完全匹配，[True, val, 'EMTY'] 无值
    #  (False, 'errmsg')
    # 格式不检查，靠查询的错误信息返回
    def check_value(self, table_name, col, val):

        cols = self.scha_loader.get_fieldInfo(table_name, col)
        if cols is None:
            return (False, f'ERROR 1054 (42S22): Unknown column {col}', '')
        ty = cols['column_type'][0]
        val = val.strip('\'"')  # 去掉原始SQL值的引号
        tup = self.is_value_exist(table_name, col, val,ttype=ty)
        return tup

    # 检查表是否存在
    def has_column(self, table_name, col_name):
        cols = self.scha_loader.get_column_nameList(table_name)
        if col_name not in cols:
            return (False, f'ERROR 1054 (42S22): Unknown column {col_name}')
        return (True, '')
           
    # vocs 字符类值的扩展vocabuary
    # (False, 'errmsg')  (True, val, 'EXCT')  (True, val, 'FUZZY')  (True, val, 'EMTY')
    def check_expn(self, table_name, col, vocs):

        exec_limit = const.D_EXPN_LIMIT
        choice = [random.randint(0, len(vocs) - 1) for _ in range(exec_limit)]
        tup = self.has_column(table_name, col)
        if tup[0] is False:
            return (False, tup[1],'')
        
        check = (True, '', 'EMTY')
        for indx in choice:
            val = f'%{vocs[indx]}%'          # 不精确匹配，直接模糊查询
            tup = self.is_value_exist(table_name, col, val)
            if tup[2] == 'FUZZY':
                return [True, val, 'EXPN']
        return check
    
    # 保持结构一致
    def make_checkDict(self,status='succ'):
        return {'status': status, 'msg': [],
                'tables': {}, 'columns': {}, 'values': []
                }

async def test(db_server, sqlList):
    
    checker = CheckSQL(dbServer=db_server)
    for sql in sqlList:
        print(f"SQL: {sql}")
        res = await checker.check_sql(sql)
        print(res)

if __name__ == "__main__":

    sqlList = [ 'SELECT *\nFROM product\nWHERE product.actual_price = 1000 AND brand = "苹果";',
                "select column_name, lang from zebura_stock_db where zebura_stock_db.lang='spain';",
                "select * from configuraciones_impresoras where brand = 'spain';",
                "select column_name, brand from zebura where brand='spain';",
                "SELECT distinct product_name, distinct price\nFROM product\nWHERE brand LIKE '%apple%';",
                "SELECT d.department_name AS Department,COUNT(e.employee_id) AS NumberOfEmployees FROM departments d  LEFT JOIN employees e ON d.department_id = e.department_id  GROUP BY d.department_name  ORDER BY  NumberOfEmployees DESC;"] 
    sql1 = """
            SELECT category, AVG(discount_percentage) AS avg_discount_rate
            FROM product
            GROUP BY category
            ORDER BY avg_discount_rate DESC
            LIMIT 1;
        """
    sql2 = """
            SELECT 
            AVG(discount_percentage) AS avg_discount_rate, category
            FROM product
            GROUP BY category
            ORDER BY avg_discount_rate DESC
            LIMIT 1 ;
        """
    sqlList.append(sql1)
    sqlList.append(sql2)
    
    dbServer = {
            'db_name':'imdb',
            'db_type':'mysql',
            'host':'localhost',
            'port':3306,
            'user':'root',
            'pwd':'zebura'
        }

    asyncio.run(test(dbServer,sqlList))
