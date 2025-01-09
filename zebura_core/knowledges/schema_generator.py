##################################################
# 扫描存放在excel里的table信息，
# 生成project schema 和 golden cases schema   
##################################################
import sys
import os
import pandas as pd
import json
sys.path.insert(0, os.getcwd())
from settings import z_config
from LLM.llm_agent import LLMAgent
import zebura_core.LLM.prompt_loader1 as ap
from utils.csv_processor import pcsv
import logging
from constants import C_PROJECT_SHEET as project
# table_name, column_name为数据库正式使用名， _zh为中文名称，没有后缀的为英文
class Scanner:
    def __init__(self):
        self.pcsv = pcsv()
        self.llm = LLMAgent()
        # self.table_keys = ["table_name", "desc", "desc_zh", "name_zh", "alias_zh", "alias", "columns"]
        # self.column_keys = ["column_name", "name_zh", "alias_zh", "alias","type", "length", "desc", "desc_zh"]
        self.tableInfo = {}
        logging.debug("Scanner init success")
        
    
    # 从csv文件中读取table信息，并对desc和alias进行补全
    # 一次只处理一个table
    async def scan_table(self, csv_filename):
        csv_rows = self.pcsv.read_csv(csv_filename)
        if csv_rows[0].get("table_name") is None:
            logging.error("table_name not found in csv")
            return None

        self.tableInfo= csv_rows[0]
        for row in csv_rows[1:]:
            column_name = row.get("column_name")
            if row.get("desc") is None or len(row.get("desc")) == 0:
                row['desc'] = await self.complete_info(column_name, "desc")
            if row.get("alias") is None or len(row.get("alias")) == 0:
                row['alias'] = await self.complete_info(column_name, "alias")

        filename = os.path.basename(csv_filename)
        csv_out = csv_filename.replace(filename, f"new_{filename}")
        
        self.pcsv.write_csv(csv_rows,csv_out)
    
    async def complete_info(self, column_name, task):
        if task not in ["desc", "alias"]:
            return None
        details = f" the term corresponds to a column name in database table {self.tableInfo['table_name']}, which is used in {self.tableInfo['desc']}. \n"
        if task == "desc":
            task = ap.tasks["term_definition"] + details
            one_shot = 'Q: "term: brand"\n'
            one_shot += 'A: a product version of it that is made by one particular manufacturer.\n'
        if task == "alias":
            task = ap.tasks["term_alias"] + details
            one_shot = 'Q: "term: brand"\n'
            one_shot += 'A: company; trademark; manufacturer\n'

        content = f"Q: {column_name}\n"
        prompt = task+one_shot
        result = await self.llm.ask_llm(content, prompt)
        if result.startswith("A:"):
            result = result[3:]
        else:
            result = ""
        return result
    
    # 所有的table信息存放在一个excel文件中，必须有一个名为project的表，存放整体信息
    # 每个sheet对应一个table
    # 格式：['no', 'table_name', 'column_name','name', 'name_zh', 'alias', 'alias_zh', 
    #       'desc', 'desc_zh', 'type', 'length','language','requirement']
    def gen_schema(self, meta_path):
        musted ={'table_name', 'column_name', 'alias', 'desc', 'type'}
        table_keys = {"table_name","alias", "desc", }
        # requirement 为结果输出时最好有的字段，否则输出结果不美观
        column_keys = {"column_name", "alias", "type", "desc","key","lang"} 

        wk_dir = os.getcwd()
        directory = os.path.join(wk_dir,meta_path)
        xls_name = os.path.join(directory, f'metadata.xlsx')
        xls = pd.ExcelFile(xls_name)

        # Get the table list
        sheet_names = xls.sheet_names
        
        recap = []
        if project in sheet_names:
            schema = self.gen_project(xls_name)
        else:
            print(f"Error: {project} sheet not found")
            return None
        
        sheet_names = set(sheet_names)
        sheet_names.remove(project)
        # Iterate over each sheet
        for sheet_name in sheet_names:
            # Read the sheet into a DataFrame
            df = pd.read_excel(xls_name, sheet_name=sheet_name)
            num_rows = df.shape[0]
            recap.append(f"{sheet_name}:table is {df.loc[0,'table_name']} ,num of columns is {num_rows-1}")
            # check the columns
            if  not musted.issubset(set(df.columns.tolist())):
                print(f"Error: incorrect columns in {sheet_name}")
                return None
            
            tb = {}
            for key in table_keys:
                tb[key] = df.loc[0,key] if not pd.isnull(df.loc[0,key]) else None
            tb = self.filter_empty(tb)
            tb["columns"] = []

            for i in range(1,num_rows):
                row = df.loc[i]
                column = {}
                for key in column_keys:
                    column[key] = row[key] if not pd.isnull(row[key]) else None
                column = self.filter_empty(column)
                if len(column.keys()) == 0:
                    break
                tb["columns"].append(column)
           
            schema["tables"].append(tb)
        # 生成project schema
        project_code = schema.get("_project_code")
        output_path = os.path.join(os.path.dirname(xls_name), f"{project_code}_meta.json")
        recap.append(f"Output path: {output_path}")
        self.write_json(schema, output_path)
        # 生成golden cases的schema
        project_code = schema.get("_project_code")
        gschema = self.gen_gcases_schema(project_code)
        output_path = os.path.join(os.path.dirname(xls_name), f"{project_code}_gcases.json")
        self.write_json(gschema, output_path)

        print(recap)
        return
        
    @staticmethod
    # project sheet每一行的格式：'key', 'value'， key存入schema时前面加'_'
    # project_name， project_code，domain，desc，possessor
    # project_code 为该项目的gcases.json, meta.json的前缀
    def gen_project(file_path) ->dict:
        
        from datetime import datetime
        now = datetime.now()
        schema = {  "_comment":f"The schema is generated through file {os.path.basename(file_path)} ,created in the {now.date()}",
                    "tables":[]
                }
        df = pd.read_excel(file_path, sheet_name=project)
        num_rows = df.shape[0]
        for i in range(0,num_rows):
            k = df.iloc[i,0]
            v = df.iloc[i,1]
            if pd.isnull(k):
                break
            if pd.isnull(v):
                v = ""
            schema['_'+k] = v
        return schema
    
    @staticmethod
    def gen_gcases_schema(project_code):
        schema = {
            "_comment": f"The schema is for golden cases of {project_code} project.",
            "tables": [
                        {
                            "table_name": project_code+"_gcases",
                            "columns": []
                        }
            ]
        }
        # sql: 模型结果； gt: ground truth 正确SQL; activity: 执行操作(这部分可写下一步的推荐)； explain: 解释； category: 类别
        fields = ["no", "query", "qemb", "sql", "gt", "activity","explain","category", "updated_date","next_query"]  
        types = ["text", "text", "dense_vector", "text", "text", "text", "text", "keyword", "date", "text"]
        for i, field in enumerate(fields):
            schema["tables"][0]["columns"].append({"column_name": field, "type": types[i]})
        
        return schema
    

    @staticmethod
    def filter_empty(dict):
        filtered_dict = {k: v for k, v in dict.items() if v and v != ""}
        return filtered_dict
    
    @staticmethod
    def write_json(dict, out_path):
        from collections import OrderedDict
        ordered_dict = OrderedDict(sorted(dict.items()))
        with open(out_path, 'w', encoding='utf-8-sig') as json_file:
            json_file.write(json.dumps(ordered_dict, indent=4, ensure_ascii=False))

# example usage
if __name__ == '__main__':
    scanner = Scanner()
    #asyncio.run(scanner.scan_table("C:\something\zebura\\training\it\dbInfo\product_info.csv"))
    scanner.gen_schema("training\stock")