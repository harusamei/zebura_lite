##########################################################
# extract tables,column, alias, conditions, check the SQL syntax
# 基于sqlparse库的扩展，解析SQL语句，提取SQL中的列名，表名，条件等信息
# 复杂SQL 由LLM处理
##########################################################
import sqlparse,sys,os,re
from sqlparse.tokens import Keyword, DML, Whitespace, Punctuation, Literal,Operator, Wildcard, Comment,Token,Name
from sqlparse.sql import IdentifierList, Identifier, Where, Function, Parenthesis,Comparison
sys.path.insert(0, os.getcwd().lower())
from zebura_core.LLM.prompt_loader1 import Prompt_generator
from zebura_core.LLM.llm_agent import LLMAgent
from zebura_core.LLM.ans_extractor import AnsExtractor
import logging

class ParseSQL:
    _is_initialized = False

    def __init__(self):
        #只初始化一次
        if not ParseSQL._is_initialized:
            ParseSQL._is_initialized = True
            self.prompter = Prompt_generator()
            self.llm = LLMAgent()
            self.ans_ext = AnsExtractor()
            self.endSet = ['LIMIT', 'OFFSET', 'ORDER BY', 'GROUP BY', 'HAVING','FETCH']
            logging.debug("ParseSQL init done")

            ParseSQL.prompter = self.prompter
            ParseSQL.llm = self.llm
            ParseSQL.ans_ext = self.ans_ext
            ParseSQL.endSet = self.endSet

    # 主函数，只处理DML中的select语句，不处理insert, update, delete, 也不处理DDL， DCL
    # "tables": [ {"name": "employees", "alias": "t1"}]
    # "columns": [{"name": "name", "table": "employees", "alias": "employee_name"}]
    # "values": [{"value": "value1", "column": "column_name1", "table": "table_name1"}]
    async def extract_sql(self, sql) -> dict:

        slots = self.make_a_slots()

        if 'select' not in sql.lower():
            slots['status'] = 'failed'
            slots['msg'] = 'ERR: extraction, only support select statement'
            return slots
         
        parsed = sqlparse.parse(sql)        
        if len(parsed) < 1:
            slots['status'] = 'failed'
            slots['msg'] = 'ERR: extraction, can not parse the SQL'
            return slots
        # 由LLM抽取含多个select语句的复杂SQL
        matches = re.finditer(re.escape('select'), sql, re.IGNORECASE)
        if len(list(matches)) > 1 or 'join' in sql.lower():
            slots = await self.extract_with_llm(sql)
            return slots
        
        tokens = filter(lambda x: not x.is_whitespace, parsed[0].tokens)
        tokens = list(tokens)
        values =[]
        for token in tokens:
            values.extend(self.get_token_values(token))
        values = [val for val in values if val['ttype'] != 'Other']
        # logging.debug(' '.join(self.travelValues(values)))
        # 流水线式解析
        loc,area = 0,''
        while loc < len(values):
            val = values[loc]
            val_name = val['name'].upper()
            if val_name == 'SELECT':
                area = 'columns'
                tResult, shift = self.get_columns(values[loc:])
            elif val_name == 'FROM':
                area = 'tables'
                tResult, shift = self.get_tables(values[loc:])
            elif val_name == 'WHERE':
                area = 'values'
                tResult, shift = self.get_sql_values(values[loc:])
            elif val_name in self.endSet:
                area = 'others'
                tResult, shift = None, 1
            else:
                area = 'skip'
                tResult, shift = None, 1
            if tResult is not None and len(tResult[area]) > 0:
                slots[area].extend(tResult[area])
            loc += shift

        if len(slots['tables'])==0:
            slots = await self.extract_with_llm(sql)
            return slots
        # alias mapping
        tb_maps,col_maps ={},{}
        df_tb = slots['tables'][0].get('name','')
        for tb in slots['tables']:
            if tb.get('alias') is not None:
                tb_maps[tb['alias']] = tb['name']
        
        for col in slots['columns']:
            if col['table'] in tb_maps.keys():
                col['table'] = tb_maps[col['table']]
            if col.get('table','') == '':
                col['table'] = df_tb
            if col.get('alias') is not None:
                col_maps[col['alias']] = f"{col['table']}.{col['name']}"

        for val in slots['values']:
            col = val['column']
            if '.' in col:
                tb, col = col.split('.',1)
            else:
                tb = df_tb            
            if col in col_maps.keys():
                col = col_maps[col]
            if tb in tb_maps.keys():
                tb = tb_maps[val['table']]
            val['column'] = col
            val['table'] = tb

        return slots
    
    async def extract_with_llm(self, sql):
        slots ={}
        tmpl = self.prompter.tasks['sql_breakdown']
        query = tmpl.format(sql_statement=sql)
        llm_answ = await self.llm.ask_llm(query, '')
        result = self.ans_ext.output_extr('sql_breakdown',llm_answ)
        if result['status'] == 'failed':
            slots['status'] = 'failed'
            slots['msg'] = 'error_LLM: some error in the SQL extraction'
        else:
            slots = result['msg']
            slots['status'] = 'succ'
            slots['msg'] = 'LLM: SQL extraction done'
        return slots
    
    def get_columns(self, vals) -> tuple:
        #格式 [{"name": "name", "table": "employees", "alias": "employee_name"}]
        columns = []
        result = {'columns': []}
        if vals[0]['name'].upper() != 'SELECT':
            return result, 1
        
        for indx, item in enumerate(vals[1:]):   
            if item['name'].upper() in ['FROM']:
                break
            name = item['name']
            enty = {'name': name, 'table': ''}
            if item['ttype'] == 'Identifier' and item.get('subValues') is None:
                columns.append(enty.copy())
            elif item.get('subValues') is not None:
                entyList = self.extract_itemSub(item, 'columns')
                columns.extend(entyList)
        
        result['columns'] = columns
        return result, indx+1  # 返回解析的列名，以及end位置
    
    # 当前结点包含的部分，当前解析区域
    def extract_itemSub(self, item, area):
        entyList = []
        valStr = item['subValues'][0]['name']
        tokens = item['subValues'][0]['tokens'] 

        if item['ttype'] == 'Function':  # 当前结点是函数，处理函数中的列名
            for tk in tokens:
                if isinstance(tk, Parenthesis):
                    tItem = {'name': tk.value, 'ttype': 'Parenthesis'}
                    tItem['subValues'] = [{'name': tk.value, 'tokens': tk.tokens}]
                    tList = self.extract_itemSub(tItem, area)
                    entyList.extend(tList)
        
        if item['ttype'] == 'Parenthesis':  # 当前结点是括号，处理括号中的列名
            for tk in tokens:
                if isinstance(tk, Identifier):
                    entyList.append({'name': tk.value, 'table': ''})
                
        if item['ttype'] == 'Identifier':  # 当前结点是标识符
            name = item['name']
            enty ={ 'name': name, 'table': ''}
            match = re.search(r'\s+AS\s+(\w+)',valStr, re.IGNORECASE)    # 列别名      
            if match:
                enty['alias'] = match.group(1)
            if f'.{name}' in valStr:
                enty['table'] = valStr.split('.')[0]          # 列所属表, 表通常使用别名
            entyList.append(enty.copy())
            if isinstance(tokens[0],Function):
                enty['table'] = 'Function'
                tItem = {'name': tokens[0].value, 'ttype': 'Function','tokens': tokens[0].tokens}
                tItem['subValues'] = [tItem]
                tList = self.extract_itemSub(tItem, area)
                entyList.extend(tList)        
        
        return entyList

    def get_tables(self,vals) -> tuple:

        tables = [] #[{'name': '', 'alias':''}]
        result = {'tables': []}
        if vals[0]['name'].upper() != 'FROM':
            return result, 1
        
        endSet = self.endSet + ['WHERE']
        for indx, item in enumerate(vals[1:]):
            if item['name'].upper() in endSet:
                break
            name = item['name']
            if item.get('subValues') is not None:
                enty={'name': name}
                subTokens = item['subValues'][0]['tokens']
                if isinstance(subTokens[0], Parenthesis):  # 去掉子查询形成表的情况
                    continue
                subVal = item['subValues'][0]['name']   # 表别名
                match = re.search(r'(\w+)\s+(AS\s+)?(\w+)',subVal, re.IGNORECASE)          # 列别名
                if match:
                    enty['alias'] = match.group(3)
                tables.append(enty.copy())
                   
        result['tables'] = tables
        return result, indx+1
    # values in where clause
    def get_sql_values(self,vals):
        result = {'values': []}
        conditions = []
        for indx, item in enumerate(vals[1:]):
            if item['name'].upper() in self.endSet:
                break
            if item['ttype'] == 'Comparison':
                conditions.append(item['name'])
            elif item['ttype'] == 'Parenthesis':
                subTokens = item['subValues'][0]['tokens']
                for tk in subTokens:
                    if isinstance(tk, Comparison):
                        conditions.append(tk.value)      
        # 正则表达式模式，用于匹配条件的三个部分：列名、运算符和值
        pat = re.compile(r'(\w+)\s*(=|!=|<>|<|<=|>|>=|LIKE|IN|BETWEEN)\s*(.+)', re.IGNORECASE)
        enty ={ 'column': '', 'table': '', 'value': '' }
        for cond in conditions:
            match = pat.match(cond)
            if match:
                enty['value'] = match.group(3).strip('\'"')
                enty['column'] = match.group(1)
                result['values'].append(enty.copy())
        return result, indx+1
    
    def get_token_values(self, token):
        values = []     #{'name': '', 'ttype': ''} 
        
        if isinstance(token, IdentifierList):
            for item in token:
                values.extend(self.get_token_values(item))
            return values
        elif isinstance(token, Where):         # where 条件
            for item in token.tokens:
                values.extend(self.get_token_values(item))
            return values

        if isinstance(token, Identifier):    #  通常表示 SQL 语句中的标识符，例如表名、列名、别名
            values.append({'name':token.get_real_name(),'ttype':'Identifier'})
        elif isinstance(token, Parenthesis):  # 括号
            values.append({'name': token.value, 'ttype': 'Parenthesis'})
        elif isinstance(token, Function):
            values.append({'name': token.value, 'ttype': 'Function'})
        elif token.ttype is Wildcard:         # 通配符
            values.append({'name': token.value, 'ttype': 'Wildcard'})
        elif token.ttype is Keyword or token.ttype is DML:          # 关键字
            values.append({'name': token.value, 'ttype': 'Keyword'})
        elif 'Literal' in str(token.ttype):          # 字面量
            values.append({'name': token.value, 'ttype': 'Literal'})
        elif isinstance(token, Comparison):        # 比较符号
            values.append({'name': token.value, 'ttype': 'Comparison'})
        elif token.ttype is Token.Keyword.Order:
            values.append({'name': token.value, 'ttype': 'Order'})
        else:                   # 所有其它情况返回空
            values.append({'name': token.value, 'ttype': 'Other'})
    
        temVal = values[-1]
        # 保留subtokens,留待进一步解析
        if temVal.get('ttype','') in ['Parenthesis','Identifier','Function']:
            if token.tokens is not None:
                subVal = {'name': token.value, 'ttype': 'subTokens'}        
                subVal['tokens'] = token.tokens                 
                temVal['subValues'] = [subVal]
        
        return values
    
    def travelValues(self, values):
        tstr=[]
        for val in values:
            tstr.append(val['name'])
            if val.get('subValues', None):
                tstr.append('[')
                tstr.extend(self.travelValues(val['subValues']))
                tstr.append(']')
        return tstr
    
    # "tables": [ {"name": "employees", "alias": "t1"}]
    # "all_cols": [{"name": "name", "table": "employees", "alias": "employee_name"}]
    # "conditions": [{"condition": "t1.age > 30"}]
    # 与LLM的JSON结果保持一致
    def make_a_slots(self):
        return {
            'status':'succ',
            'columns': [],          # select...from 之间, {'name':'','table':'','alias':''}
            'tables': [],           # from...where 之间,{'name': '','alias':''}
            'values': [],           # where 部分，出现的值
            'msg':''
        }

async def test(sql_querys, output_file):

    sparser = ParseSQL()
    with open(output_file, "w") as file:
        for sql in sql_querys:
            print("SQL:", sql)
            slots = await sparser.extract_sql(sql)
            
            file.write(sql + "\n")
            slots_str = json.dumps(slots, indent=4)
            file.write(slots_str + "\n")
            file.write("-------------------------------------------------\n")
    print("Output written to", output_file)
    return


# example usage
if __name__ == '__main__':
    import json
    import asyncio
    sql_querys = """
    SELECT column1 FROM product LIMIT 10 OFFSET 20;
    SELECT column1,column2 FROM product1 LIMIT 10 OFFSET 20;
    select * from tableOne;
    SELECT category, COUNT(rating) AS rating_count, AVG(rating) AS average_rating FROM product WHERE category LIKE '%fan%' group by category;   
    SELECT t1.name AS employee_name, t2.salary FROM employees t1 JOIN salaries t2 ON t1.id = t2.employee_id WHERE t1.age > 30 AND (t2.salary > 50000 OR t1.department = 'HR');
    SELECT COUNT(DISTINCT category) FROM product;
    SELECT DISTINCT customer_id AS ID,  first_name AS FirstName,  last_name AS LastName, city AS City FROM  customers  AS CUS ORDER BY  City ASC, LastName DESC;
    SELECT d.department_name ,COUNT(e.employee_id) AS NumberOfEmployees FROM departments d  LEFT JOIN employees e ON d.department_id = e.department_id  GROUP BY d.department_name  ORDER BY  NumberOfEmployees DESC;
    SELECT d.department_name, COUNT(d.employee_id) AS NumberOfEmployees FROM departments d GROUP BY d.department_name;    
    SELECT order_id, customer_id, order_date, total_amount FROM  orders  WHERE  order_date BETWEEN '2024-01-01' AND '2024-12-31'  ORDER BY  order_date ASC;
    SELECT column1 FROM tableOne ORDER BY column1 FETCH FIRST 10 ROWS ONLY;
    select column1 FROM tableOne;
    SELECT DISTINCT column1 FROM tableOne;
    SELECT column1 FROM tableOne WHERE column2 = 'value' AND column3 = 'value' OR column4 = 'value';
    SELECT column1, COUNT(column2) FROM tableOne GROUP BY column1;    
    SELECT column1, COUNT(*) FROM tableOne GROUP BY column1 HAVING COUNT(*) > 1;
    SELECT column1 FROM tableOne ORDER BY column1 ASC;
    SELECT column1 AS renamed_column1, column2 AS renamed_column2 FROM tableOne;
    UPDATE tableOne SET column1 = value1, column2 = value2 WHERE condition;
    SELECT * FROM employees WHERE (department = 'Sales' or salary > 10000) AND department = 'Marketing';
    SELECT * FROM products AS PP  WHERE product_name LIKE '%apple%' AND price > 1000;
    SELECT outer_column1, outer_column2
            FROM (
                SELECT inner_column1 AS outer_column1, inner_column2 AS outer_column2
                FROM inner_table
                WHERE inner_column3 = 'some_value'
            ) AS inner_query
            WHERE outer_column1 > 10;
    """
    sql_querys=sql_querys.split(';')
    sql_querys = [sql.strip() for sql in sql_querys if sql.strip() != '']
    output_file = "c:/something/talq/zebura_db/zebura_core/utils/sql_output.txt"
    asyncio.run(test(sql_querys, output_file))
    
