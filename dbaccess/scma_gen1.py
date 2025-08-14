# 一个database为一个项目，生成该DB下所有表的元信息
# 导出到metadata.xlsx, 用于人工修正
##########################################
import sys,os
sys.path.insert(0, os.getcwd().lower())
import zebura_core.constants as const
from zebura_core.LLM.prompt_loader1 import Prompt_generator
from zebura_core.LLM.llm_agent import LLMAgent
from zebura_core.LLM.ans_extractor import AnsExtractor
from zebura_core.utils.lang_detector import langcode2name, detect_language
from dbaccess.db_ops1 import DBops
from datetime import datetime, date
import json
import random
import asyncio
import pandas as pd

def default_serializer(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")

class ScmaGen:
    # 需要明确指定数据库及生成内容所用语言
    # 为获得最佳性能生成的元信息所用语言与prompt语言和应用时交互语言一致最好
    def __init__(self, dbServer, lang='English'):
        if dbServer is None:
            raise ValueError("db_name must be specified")
        
        self.ops = DBops(dbServer)
        self.db_name = dbServer['db_name']
        self.db_type = dbServer['db_type']

        self.prompter = Prompt_generator(lang=lang)
        self.ans_extr = AnsExtractor()
        self.llm = LLMAgent()
        
        self.lang = lang                            # 生成元信息所用语言
        self.MAX_GROUP = 10                         # 最大group数
        self.MAX_TAG = 15                           # 最大tag数
        self.MAX_HYPERFIELD = 100                   # 最大hyperfield数,上位词
        
        self.database = const.Z_META_PROJECT        # 项目信息
        self.fields = const.Z_META_FIELDS           # 字段信息
        self.tables = const.Z_META_TABLES           # 表信息
        self.terms = const.Z_META_TERMS             # 术语表

        self.scma_dfs = None
         
    # 从SQL中读一张表的结构, 生成表结构信息保存入self.scma_dfs
    def gen_tb_scma(self, tb_name):
        
        tb_scma = self.ops.show_tb_schema(tb_name)
        print(f"Table: {tb_name}, Columns: {len(tb_scma)}")
        
        self.scma_dfs['tables'] = pd.concat([self.scma_dfs['tables'], pd.DataFrame([{'table_name': tb_name, 'column_count': len(tb_scma)}])], ignore_index=True)
        tb_df = self.scma_dfs['tables']

        result = self.ops.show_randow_rows(tb_name, 3)
        result = result.mappings().all()
        col_names = result[0].keys()
        tb_df.loc[tb_df['table_name'] == tb_name, 'tb_prompt'] = ','.join(col_names)
        # 字段名所用语言为tb_lang
        langCode = detect_language(' '.join(col_names))
        if langCode is not None:
            lang = langcode2name(langCode)
            tb_df.loc[tb_df['table_name'] == tb_name, 'tb_lang'] = lang
        
        result_dicts = [dict(row) for row in result]  # Convert RowMapping objects to dictionaries
        # examples dataframe
        ex_df = pd.DataFrame(result_dicts)
        
        tStr = json.dumps(result_dicts, ensure_ascii=False, default=default_serializer)
        # print(json.loads(tStr))
        tb_df.loc[tb_df['table_name'] == tb_name, 'examples'] = tStr
        # 从字典生成dataframe
        fd_df = pd.DataFrame(tb_scma,columns=['column_name','data_type','character_maximum_length']) 
        fd_df['table_name'] = tb_name
        fd_df.rename(columns={'data_type':'column_type','character_maximum_length':'column_length'}, inplace=True)
        for col in ['column_name','column_type']:
            fd_df[col] = fd_df[col].astype('object')
        # 添加实例和实例所用语言
        for key in ex_df.columns.tolist():
            one_col = ex_df[key].tolist()
            fd_df.loc[fd_df['column_name']==key, 'examples'] = str(one_col)
            # 只有字符类型才检测语言
            column_type_series = fd_df.loc[fd_df['column_name'] == key, 'column_type']
            # Check for 'char' or 'text' in the column_type
            str_flag = column_type_series.str.contains('char', case=False).any()
            str_flag = str_flag or column_type_series.str.contains('text', case=False).any()

            if str_flag:
                langCode = detect_language(' '.join(one_col))
                if langCode is not None:
                    lang = langcode2name(langCode)
                    fd_df.loc[fd_df['column_name'] == key, 'val_lang'] = lang
            
        result = self.ops.show_primary_key(tb_name)
        primary_keys = result.fetchall()
        primary_keys = [pk[0] for pk in primary_keys]
        if primary_keys:
            fd_df.loc[fd_df['column_name'].isin(primary_keys), 'column_key'] = 'PRI'
        else:
            fd_df['column_key'] = ''

        self.scma_dfs['fields'] = pd.concat([self.scma_dfs['fields'], fd_df], ignore_index=True)
        return
    
    # 从数据库中读取所有表的schema信息
    def gen_db_info(self, meta_xls):
        self.scma_dfs= {'tables':pd.DataFrame(columns = self.tables), 
                        'fields':pd.DataFrame(columns = self.fields),
                        'database':pd.DataFrame(columns = self.database),
                        'terms':pd.DataFrame(columns = self.terms)
                        }
        tpj = {'database_name':self.db_name,
                'chat_lang':self.lang,
                'possessor': const.C_SOFTWARE_NAME
        }
        self.scma_dfs['database'] = pd.concat([self.scma_dfs['database'], pd.DataFrame([tpj])], ignore_index=True)

        # 生成三张表: database, tables, fields
        tables = self.ops.show_tables()
        tables = [table[0] for table in tables]
        print(f"Database: {self.db_name}, Tables: {len(tables)}")
        count = 0
        for table_name in tables:
            self.gen_tb_scma(table_name)
            print(count, table_name)
            count += 1
        # Check if the meta_xls file exists, and delete it if it does
        if os.path.exists(meta_xls):
            os.remove(meta_xls)
            print(f"Existing file {meta_xls} has been deleted.")
        self.output_schma(meta_xls)
        print(f"the schema information of each table are saved to {meta_xls}")
        return
    
    # table grouping and tagging
    async def tb_enhance(self, meta_xls):
        self.scma_dfs = pd.read_excel(meta_xls, sheet_name=None)
        tb_df = self.scma_dfs['tables']
        tb_df['group_name'] = tb_df['group_name'].astype('object')
        tb_df['tags'] = tb_df['tags'].astype('object')
        gt_df = self.scma_dfs['terms']
        groupList = gt_df[gt_df['ttype']=='group'].to_dict(orient='records')
        tagList = gt_df[gt_df['ttype']=='tag'].to_dict(orient='records')

        grpDict = {group['term_name']:[] for group in groupList}
        tagDict = {tag['term_name']:[] for tag in tagList}

        groupList = [f"{group['term_name']}: {group['term_desc']}" for group in groupList]
        tagList = [f"{tag['term_name']}: {tag['term_desc']}" for tag in tagList]
        group_Info = '\n'.join(groupList)
        tag_Info = '\n'.join(tagList)

        # 用整理好的standard group和tag更新表信息
        tmpl = self.prompter.tasks['tb_classification']
        for _, row in tb_df.iterrows():
            print(row['table_name'])
            samples = json.loads(row['examples'])
            s_df = pd.DataFrame(samples)
            tb_md = s_df.to_markdown(index=False)
            table_info = f"table name:{row['table_name']}\ncolumns and samples:\n{tb_md}"
            query = tmpl.format(table_info=table_info, group_info=group_Info, tag_info=tag_Info)
            print(f"length of query: {len(query)}")
            llm_answ = await self.llm.ask_llm(query, '')
            with open('tem.out', 'a', encoding='utf-8') as f:
                f.write(query + '\n')
                f.write(llm_answ + '\n------------\n')

            result = self.ans_extr.output_extr('tb_enhance', llm_answ)
            if result['status'] == 'failed' or 'group' not in result['msg']:
                print(f"Failed to extract llm answer: {llm_answ}")
                continue
            result = result['msg']
            tb_df.loc[tb_df['table_name']==row['table_name'], 'group_name'] = result['group']
            tb_df.loc[tb_df['table_name']==row['table_name'], 'tags'] = ', '.join(result['tags'])
            tb_name = row['table_name']
            if result['group'] not in grpDict:
                print(f"Group not found: {result['group']}")
            else:
                grpDict[result['group']].append(tb_name)
            
            for tag in result['tags']:
                if tag not in tagDict:
                    print(f"Tag not found: {tag}")
                else:
                    tagDict[tag].append(tb_name)

        term_df = self.scma_dfs['terms']
        term_df['related_tables'] = term_df['related_tables'].astype('object')
        for group in grpDict:
            term_df.iloc[term_df[term_df['term_name']==group].index, term_df.columns.get_loc('related_tables')] = ', '.join(grpDict[group])
        for tag in tagDict:
            term_df.iloc[term_df[term_df['term_name']==tag].index, term_df.columns.get_loc('related_tables')] = ', '.join(tagDict[tag])

        self.output_schma(meta_xls)
        print(f"the grouping and tagging information of each table are saved to {meta_xls}")
        return

    # 给表的字段增加上位词，用于抽象表
    async def field_enhance(self, meta_xls):

        self.scma_dfs = pd.read_excel(meta_xls, sheet_name=None)
        fd_df = self.scma_dfs['fields']
        term_df = self.scma_dfs['terms']
        hypernyms = term_df[term_df['ttype']=='field'].to_dict(orient='records')

        # 把filed 分拣到各个上位词下
        cat_sorter = {hyperItem['term_name']: [] for hyperItem in hypernyms}
        hypernym_list = ', '.join([f"{hyperItem['term_name']}" for hyperItem in hypernyms])
        hypernym_list = f"[ {hypernym_list} ]"
        selected = ['table_name','column_name','column_type','val_lang','examples']
        tmpl = self.prompter.tasks['column_hypernym']
        # colnum_name: hypernym
        col_hyper = {}
        print(f"len of fd_df: {len(fd_df)}")
        for i in range(0, len(fd_df), 3):
            print(f"Batch: {i}")
            batch_df = fd_df.iloc[i:i+4]
            column_info = batch_df[selected].to_markdown(index=False)
            query = tmpl.format(column_info=column_info, hypernym_list=hypernym_list)
           
            llm_answ = await self.llm.ask_llm(query, '')
            result = self.ans_extr.output_extr('column_hypernym', llm_answ)

            # with open('tem.out', 'a', encoding='utf-8') as f:
            #     f.write(query + '\n')
            #     f.write(llm_answ + '\n------------\n')
            
            if result['status'] == 'failed' or (not isinstance(result['msg'], list)):
                continue
            result = result['msg']
            for item in result:
                if 'column_name' not in item or 'assigned_hypernym' not in item:
                    print(f"Failed to get column name or assigned hypernym: {item}")
                    continue
                col_name = item.get('column_name')
                asgd_hyper = item.get('assigned_hypernym')
                
                # 可能存在不同表的相同列名，在other_cols中已经存在，col_name与表名无关，且最后一个hypernym有效
                col_hyper[col_name] = asgd_hyper
                if batch_df[batch_df['column_name']==col_name].empty:
                    print(f"Column not found: {col_name}")
                    continue
                tb_name = batch_df[batch_df['column_name']==col_name]['table_name'].values[0]
                if pd.isna(tb_name):
                    print(f"Table name is null: {col_name}")
                    continue
                if asgd_hyper not in cat_sorter:
                    print(f"Hypernym not found: {col_name}, {asgd_hyper}")
                else:
                    cat_sorter[asgd_hyper].append(tb_name)
            
        print(f"len of other_cols: {len(col_hyper)}")
        
        for column_name, hypernym in col_hyper.items():
            fd_df.loc[fd_df[fd_df['column_name']==column_name].index, 'hypernym'] = hypernym
            tList = fd_df[fd_df['column_name']==column_name]['table_name'].tolist()
            print(f"Column: {column_name}, Hypernym: {hypernym}, Tables: {len(tList)}")
        for hypernym,tList in cat_sorter.items():
            tList = list(set(tList))
            term_df.loc[term_df[term_df['term_name']==hypernym].index, 'related_tables'] = ', '.join(tList)

        self.output_schma(meta_xls)
        print(f"the hypernym information of each field are saved to {meta_xls}")

        return
    
    # 生成表和字段的描述
    async def table_description(self, meta_xls):
        self.scma_dfs = pd.read_excel(meta_xls, sheet_name=None)
        fd_df = self.scma_dfs['fields']
        fd_df['column_desc'] = fd_df['column_desc'].astype('object')
        tb_df = self.scma_dfs['tables']
        tb_df['tb_desc'] = tb_df['tb_desc'].astype('object')
        tmpl = self.prompter.tasks['column_description']
        for _, row in tb_df.iterrows():
            tb_name = row['table_name']
            print(f"Table: {tb_name}")
            tdf = fd_df[fd_df['table_name']==tb_name]
            tdf = tdf[['column_name', 'column_type', 'val_lang', 'examples']]
            tmd = tdf.to_markdown(index=False)
            tb_info = f"table name:{tb_name}\ncolumns and samples:\n{tmd}"
            query = tmpl.format(tb_info=tb_info,chat_lang=self.lang)
            llm_answ = await self.llm.ask_llm(query, '')
            result = self.ans_extr.output_extr('field_description', llm_answ)
            if result['status'] == 'failed':
                print(f"Failed to extract llm answer: {llm_answ}")
                continue
            result = result['msg']
            if isinstance(result, str):
                print(f"Failed to get column name or description: {result}")
                continue
            tb_desc = result.get('table_description')
            if tb_desc is None:
                print(f"Failed to get table description: {result}")
                continue
            tb_df.loc[tb_df['table_name'] == tb_name, 'tb_desc'] = tb_desc
            result = result.get('columns')
            if result is None:
                print(f"Failed to get columns: {result}")
                continue
            for item in result:
                if isinstance(item, str):
                    print(f"Failed to get column name or description: {item}")
                    continue
                col_name = item.get('column_name')
                col_desc = item.get('description')
                if col_name is None or col_desc is None:
                    print(f"Failed to get column name or description: {item}")
                    continue
                fd_df.loc[(fd_df['column_name'] == col_name) & (fd_df['table_name'] == tb_name), 'column_desc'] = col_desc        
        self.output_schma(meta_xls)
        print(f"the description of each field are saved to {meta_xls}")
        return
    
    # 通过LLM定义tables的 group list，tag list
    async def define_groups_tags(self, meta_xls):

        self.scma_dfs = pd.read_excel(meta_xls, sheet_name=None)
        # 生成table grouping
        tb_df = self.scma_dfs['tables']
        pj_df = self.scma_dfs['database']
        db_name = pj_df['database_name'][0] #表的第一行
        # 采用随机采样，处理表个数很多时情况
        table_count = tb_df.shape[0]
        one_permutation = random.sample(range(table_count), table_count)
        infoList = []
        tb_info = []
        allLen = 0
        for i in range(table_count):
            row = tb_df.iloc[one_permutation[i]]
            tStr = f"table name:{row['table_name']}\ncolumn names: {row['tb_prompt']}"
            tb_info.append(tStr)
            allLen += len(tStr)
            if allLen > const.D_MAX_PROMPT_LEN:
                infoList.append({'allLen':allLen, 'tb_info':'\n---------\n'.join(tb_info)})
                tb_info = []
                allLen = 0
        if allLen > 0:
            infoList.append({'allLen':allLen, 'tb_info':'\n---------\n'.join(tb_info)})

        tmpl = self.prompter.tasks['db_glossary']
        aswList= []
        for tItem in infoList:
            query = tmpl.format(db_name=db_name, tables_info=tItem['tb_info'])
            llm_answ = await self.llm.ask_llm(query, '')
            # with open('tem.out', 'a', encoding='utf-8') as f:
            #     f.write(query + '\n')
            result = self.ans_extr.output_extr('db_glossary',llm_answ)
            if result['status'] == 'failed':
                print(f"Failed to extract llm answer: {llm_answ}")
            else:
                aswList.append(result['msg'])
        
        # 生成 group list and tag list
        groups,tags, tables = [],[],[]
        for asw in aswList:
            if not isinstance(asw, dict) or 'groups' not in asw:
                print(f"Failed to get glossary: {asw}")
                continue
            groups.extend(asw['groups'])
            tags.extend(asw['tags'])
            tables.extend(asw['tables'])
        
        groupInfo, tagInfo = {}, {}
        for group in groups:
            groupInfo[group['group_name']] = group['description']   
        for tag in tags:
            tagInfo[tag['tag_name']] = tag['description']

        print(f"len of groupInfo: {len(groupInfo)}, len of tagInfo: {len(tagInfo)}")
        groupInfo = [f"{name}: {desc}" for name, desc in groupInfo.items()]
        tagInfo = [f"{name}: {desc}" for name, desc in tagInfo.items()]
        # 生成group list and tag list
        groups = await self.term_normalization(self.MAX_GROUP, '\n'.join(groupInfo))
        tags = await self.term_normalization(self.MAX_TAG, '\n'.join(tagInfo))
        
        if groups is None or tags is None:
            print("Failed to generate group and tag list")
            return
        
        gt_df = pd.DataFrame(groups, columns=['standard_term','description'])
        gt_df['ttype'] = 'group'
        tag_df = pd.DataFrame(tags, columns=['standard_term','description'])
        tag_df['ttype'] = 'tag'
        print(tag_df.to_dict(orient='records'))
        
        gt_df = pd.concat([gt_df, tag_df], ignore_index=True)
        gt_df.rename(columns={'standard_term':'term_name','description':'term_desc'}, inplace=True)
        term_df = self.scma_dfs['terms']
        term_df['term_name'] = gt_df['term_name']
        term_df.update(gt_df[['term_desc', 'ttype']], overwrite=True)
        
        self.output_schma(meta_xls)
        print(f"Group and tag list are saved to {meta_xls}")
        return
    
    # catInfo 为group和tag的描述信息, 包含 term_name, ndescription(optional)
    async def term_normalization(self, max_cats, catInfo):
        # 生成group list and tag list
        tmpl = self.prompter.tasks['term_normalization']
        query = tmpl.format(max_cats = max_cats, catInfo=catInfo)
        llm_answ = await self.llm.ask_llm(query, '')
        result = self.ans_extr.output_extr('term_normalization', llm_answ)
        with open('tem.out', 'a', encoding='utf-8') as f:
            f.write(query + '\n')

        if result['status'] != 'failed':
            return result['msg']
        else:
            return None
        
    # 生成column names的上位词
    async def define_hyperfield(self, meta_xls):

        self.scma_dfs = pd.read_excel(meta_xls, sheet_name=None)
        fd_df = self.scma_dfs['fields']
        term_df = self.scma_dfs['terms']
        tag_df = term_df[term_df['ttype']=='tag']
        # 字段名分批次ask llm， 考虑超多表情况
        batch = []
        allLen = 0
        for _, row in tag_df.iterrows():
            fieldInfo = []
            allLen = 0
            if row['related_tables'] == '' or pd.isna(row['related_tables']):
                continue
            related_tb = row['related_tables'].split(',')
            print(f"Tag: {row['term_name']}, Related tables: {len(related_tb)}")
            for tb_name in related_tb:
                fieldInfo.append(f"table name:{tb_name}")
                tdf = fd_df[fd_df['table_name']==tb_name]
                tdf = tdf[['column_name', 'column_type', 'val_lang', 'examples']]
                tmd = tdf.to_markdown(index=False)
                fieldInfo.append(tmd)
                allLen += len(tmd)
                if allLen > const.D_MAX_PROMPT_LEN:
                    batch.append(fieldInfo)
                    fieldInfo = []
                    allLen = 0
            if allLen > 0:
                batch.append(fieldInfo)
        
        print(f"len of batch: {len(batch)}")
        # column grouping
        tmpl = self.prompter.tasks['column_grouping']
        hypernym = []
        for b in batch:
            query = tmpl.format(column_info='\n'.join(b))
            llm_answ = await self.llm.ask_llm(query, '')
            with open('tem.out', 'a', encoding='utf-8') as f:
                f.write(query + '\n')
            result = self.ans_extr.output_extr('column_grouping', llm_answ)
            if result['status'] == 'failed':
                print(f"Failed to extract llm answer: {llm_answ}")
                continue
            result = result['msg']
            tList = [grp['group_name'] for grp in result]
            hypernym.extend(tList)
        hypernym = list(set(hypernym))
        print(f"len of hypernym is {len(hypernym)}")
        colInfo = [f"term name: {term}" for term in hypernym]
        hyperterms = await self.term_normalization(self.MAX_HYPERFIELD, '\n'.join(colInfo))
        if hyperterms is None:
            print("Failed to generate hypernym list")
            return
        print(f"len of standard hyperterms is {len(hyperterms)}")
        hyper_df = pd.DataFrame(hyperterms, columns=['standard_term','description'])
        hyper_df.rename(columns={'standard_term':'term_name','description':'term_desc'}, inplace=True)
        hyper_df['ttype'] = 'field'
        term_df = self.scma_dfs['terms']
        self.scma_dfs['terms'] = pd.concat([term_df, hyper_df], ignore_index=True)

        self.output_schma(meta_xls)
        return

    # db全景描述及group prompt
    async def db_description(self, meta_xls):
        self.scma_dfs = pd.read_excel(meta_xls, sheet_name=None)        
        term_df = self.scma_dfs['terms']
        gp_df = term_df[term_df['ttype']=='group']
        tb_df = self.scma_dfs['tables']
        db_df = self.scma_dfs['database']
        db_name = db_df['database_name'][0]
        dbInfo = []
        for _, row in gp_df.iterrows():
            gp_name = row['term_name']
            related_tables = row['related_tables']
            if related_tables == '' or pd.isna(related_tables):
                continue
            grp_tables = {vocab.strip() for vocab in related_tables.split(',')}
            print(f"Group: {gp_name}, Related tables: {grp_tables}")
            grp_prompt = []
            hyper_df = term_df[term_df['ttype']=='field']
            for _, r in hyper_df.iterrows():
                hyper_tables = r['related_tables']
                if hyper_tables == '' or pd.isna(hyper_tables):
                    continue
                hyper_tables = {vocab.strip() for vocab in hyper_tables.split(',')}
                if hyper_tables & grp_tables:
                    print(f"Hyper tables found: {hyper_tables & grp_tables}")
                    grp_prompt.append(r['term_name'])
                
            grp_prompt = list(set(grp_prompt))
            grp_prompt = ','.join(grp_prompt)
            tb_df.loc[tb_df['group_name'] == gp_name, 'grp_prompt'] = grp_prompt
            dbInfo.append(f"group name: {gp_name}\ndescription: {row['term_desc']}\n column information: {grp_prompt}")
        
        dbInfo = '\n'.join(dbInfo)
        print(f"len of dbInfo: {len(dbInfo)}")
        tmpl = self.prompter.tasks['db_description']
        query = tmpl.format(db_name=db_name,db_info=dbInfo,chat_lang=self.lang)
        
        llm_answ = await self.llm.ask_llm(query, '')
        result = self.ans_extr.output_extr('db_description', llm_answ)
        if result['status'] == 'failed':
            print(f"Failed to extract llm answer: {llm_answ}")
            return
        result = result['msg']
        db_df['db_desc'] = db_df['db_desc'].astype('object')
        db_df.loc[0, 'db_desc'] = result['description']
        self.output_schma(meta_xls)
        print(f"Database description is saved to {meta_xls}")
        return

    def output_schma(self,xls_name):
        writer = pd.ExcelWriter(f'{xls_name}')
        for tb_name, df in self.scma_dfs.items():
            df = df.fillna('')
            tdict = df.to_dict(orient='records')
            print(f"Table: {tb_name}, Rows: {len(tdict)}")
            df.to_excel(writer, sheet_name=f'{tb_name}', index=False)
        writer.close()
        return xls_name

# Example usage
if __name__ == '__main__':

    from zebura_core.placeholder import make_dbServer

    s_name = 'Mysql1'
    dbServer = make_dbServer(s_name)
    dbServer['db_name'] = 'ebook'
    # 创建存放文件的目录
    out_path=f'{const.S_TRAINING_PATH}/{dbServer["db_name"]}'
    wk_dir = os.getcwd()
    directory = os.path.join(wk_dir,out_path)
    if not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)
    xls_name = os.path.join(directory, f'{const.S_METADATA_FILE}')  
    
    mg = ScmaGen(dbServer,'Chinese')
    # 1. 从数据库中读取所有表的schema信息
    mg.gen_db_info(xls_name)
    # 2. 生成table grouping
    asyncio.run(mg.define_groups_tags(xls_name))
    asyncio.run(mg.tb_enhance(xls_name))
    # 3. 生成field 上位词
    asyncio.run(mg.define_hyperfield(xls_name))
    asyncio.run(mg.field_enhance(xls_name))
    # 4. 生成 table, db描述
    asyncio.run(mg.table_description(xls_name))
    asyncio.run(mg.db_description(xls_name))
    print('done')
