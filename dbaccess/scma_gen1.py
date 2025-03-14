# 一个database为一个项目，生成该DB下所有表的结构
# 导出到metadata.xlsx, 用于人工修正
# 基类，不负责具体的数据库连接
##########################################
import sys,os
sys.path.insert(0, os.getcwd().lower())
import asyncio
import pandas as pd
import re
import zebura_core.constants as const
from zebura_core.LLM.prompt_loader1 import Prompt_generator
from zebura_core.LLM.llm_agent import LLMAgent
from zebura_core.LLM.ans_extractor import AnsExtractor
from zebura_core.utils.lang_detector import langcode2name, detect_language
from dbaccess.db_ops1 import DBops
from datetime import datetime, date
import time
import json
import itertools
import random

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
        
        self.lang = lang
        self.MAX_GROUP = 10                         # 最大group数
        self.MAX_TAG = 15                           # 最大tag数
        self.MAX_HYPERFIELD = 100                    # 最大hyperfield数
        
        self.database = const.Z_META_PROJECT        # 项目信息表
        self.fields = const.Z_META_FIELDS           # 字段信息
        self.tables = const.Z_META_TABLES           # 表信息
        self.terms = const.Z_META_TERMS             # 术语表
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
             
        
    # 从SQL中读一张表的结构, 生成表结构信息保存入self.scma_dfs
    def gen_tb_scma(self, tb_name):
        
        result = self.ops.show_tb_schema(tb_name)
        tb_scma = result.mappings().all()  # 将结果转换为字典格式
        print(f"Table: {tb_name}, Columns: {len(tb_scma)}")
        
        self.scma_dfs['tables'] = pd.concat([self.scma_dfs['tables'], pd.DataFrame([{'table_name': tb_name, 'column_count': len(tb_scma)}])], ignore_index=True)
        tb_df = self.scma_dfs['tables']

        result = self.ops.show_randow_rows(tb_name, 5)
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
        tb_df.loc[tb_df['table_name'] == tb_name, 'sample_data'] = tStr
        
        fd_df = pd.DataFrame(tb_scma,columns=['column_name','data_type','character_maximum_length']) # 从字典生成dataframe
        fd_df['table_name'] = tb_name
        fd_df.rename(columns={'data_type':'column_type','character_maximum_length':'column_length'}, inplace=True)
        # 添加实例和实例所用语言
        for key in ex_df.columns.tolist():
            one_col = ex_df[key].tolist()
            fd_df.loc[fd_df['column_name']==key, 'sample_data'] = str(one_col)
            # 只有字符类型才检测语言
            str_flag = fd_df.loc[fd_df['column_name'] == key, 'column_type'].str.contains('char', case=False).any()
            str_flag = str_flag or fd_df.loc[fd_df['column_name'] == key, 'column_type'].str.contains('text', case=False).any()
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
    
    def gen_db_info(self):
        # 生成三张表: database, tables, fields
        tables = self.ops.show_tables()
        tables = [table[0] for table in tables]
        print(f"Database: {self.db_name}, Tables: {len(tables)}")
        count = 0
        for table_name in tables:
            self.gen_tb_scma(table_name)
            print(count, table_name)
            count += 1
        return
    
    async def tb_enhance(self, meta_xls):
        self.scma_dfs = pd.read_excel(meta_xls, sheet_name=None)
        tb_df = self.scma_dfs['tables']
        tb_df['group'] = tb_df['group'].astype('object')
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
            samples = json.loads(row['sample_data'])
            s_df = pd.DataFrame(samples)
            tb_md = s_df.to_markdown(index=False)
            table_info = f"table name:{row['table_name']}\ncolumns and samples:\n{tb_md}"
            query = tmpl.format(table_info=table_info, group_info=group_Info, tag_info=tag_Info)
            # with open('tem.out', 'a', encoding='utf-8') as f:
            #     f.write(query + '\n')
            print(f"length of query: {len(query)}")
            llm_answ = await self.llm.ask_llm(query, '')
            result = self.ans_extr.output_extr('tb_enhance', llm_answ)
            if result['status'] == 'failed':
                print(f"Failed to extract llm answer: {llm_answ}")
                continue
            result = result['msg']
            tb_df.loc[tb_df['table_name']==row['table_name'], 'group'] = result['group']
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
        selected = ['table_name','column_name','column_type','val_lang','sample_data']
        tmpl = self.prompter.tasks['column_hypernym']
        # colnum_name: hypernym
        col_hyper = {}
        print(f"len of fd_df: {len(fd_df)}")
        for i in range(0, len(fd_df), 8):
            print(f"Batch: {i}")
            batch_df = fd_df.iloc[i:i+4]
            column_info = batch_df[selected].to_markdown(index=False)
            query = tmpl.format(column_info=column_info, hypernym_list=hypernym_list)
            with open('tem.out', 'a', encoding='utf-8') as f:
                f.write(query + '\n')

            llm_answ = await self.llm.ask_llm(query, '')
            result = self.ans_extr.output_extr('column_hypernym', llm_answ)
            if result['status'] == 'failed':
                print(f"Failed to extract llm answer: \n{llm_answ}")
                continue
            result = result['msg']
            print(result)
            for item in result:
                if isinstance(item, str):
                    print(f"Failed to get column name or assigned hypernym: {item}")
                    continue
                col_name = item.get('column_name')
                asgd_hyper = item.get('assigned_hypernym')
                if col_name is None or asgd_hyper is None:
                    print(f"Failed to get column name or assigned hypernym: {item}")
                    continue
                # 可能存在不同表的相同列名，在other_cols中已经存在，col_name与表名无关，且最后一个hypernym有效
                col_hyper[col_name] = asgd_hyper
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


    # 通过LLM定义tables的 group list，tag list
    async def define_groups_tags(self, meta_xls):

        self.scma_dfs = pd.read_excel(meta_xls, sheet_name=None)
        # 生成table grouping
        tb_df = self.scma_dfs['tables']
        pj_df = self.scma_dfs['database']
        db_name = pj_df['database_name'][0] 
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

        tmpl = self.prompter.tasks['tb_grouping']
        aswList = []
        for tItem in infoList:
            query = tmpl.format(chat_lang=self.lang, db_name=db_name, table_count=table_count, tables_info=tItem['tb_info'])
            # with open('tem.out', 'a', encoding='utf-8') as f:
            #     f.write(query + '\n')
            llm_answ = await self.llm.ask_llm(query, '')
            aswList.append(llm_answ)
        
        # 生成 group list and tag list
        groups,tags = [],[]
        for asw in aswList:
            result = self.ans_extr.output_extr('tb_grouping',llm_answ)
            if result['status'] == 'failed':
                print(f"Failed to extract llm answer: {asw}")
                continue
            result = result['msg']
            groups.extend(result['groups'])
            tags.extend(result['tables'])
        
        groupInfo = []
        for group in groups:
            tStr = f"term name: {group['group_name']}\ndescription: {group['description']}"
            groupInfo.append(tStr)

        tagList = []
        for tag in tags:
            tagList.extend(tag['tags'])
        tagList = set(tagList)
        tagInfo = [f"term name: {tag}" for tag in tagList]

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
        gt_df = pd.concat([gt_df, tag_df], ignore_index=True)
        term_df = self.scma_dfs['terms']
        term_df.update(gt_df[['standard_term', 'description', 'ttype']].rename(columns={
                            'standard_term': 'term_name',
                            'description': 'term_desc',
                            'ttype': 'ttype'
        }))
        
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
                tdf = tdf[['column_name', 'column_type', 'val_lang', 'sample_data']]
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
            # with open('tem.out', 'a', encoding='utf-8') as f:
            #     f.write(query + '\n')
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

    # summary words 数限制
    async def summary_prompt(self, meta_xls, limit_length):
        # 词长转换为字符长
        meta_dfs = pd.read_excel(meta_xls, sheet_name=None)
        pj_df = meta_dfs['database']
        tb_df = meta_dfs['tables']

        prompts =[]
        pj_desc = [f"data is stored in: {pj_df['database_name'][0]}"]
        tb_count = tb_df.shape[0]

        uniq_groups = tb_df['group_name'].unique()
        group_limit = limit_length/(len(uniq_groups)+1)
        tmpl = self.prompter.tasks['db_summary']
        for gname in uniq_groups:
            prompts = []
            prompts.append(f"group name:{gname}")
            g_df = tb_df[tb_df['group_name'] == gname]
            for _, row in g_df.iterrows():
                prompts.append(f"table name:{row['table_name']}")
                prompts.append(row['tb_prompt'])
                prompts.append('------------')
            query = tmpl.format(chat_lang = self.lang, limit_length=group_limit, db_info='\n'.join(prompts))
            llm_answ = await self.llm.ask_llm(query, '')
            llm_answ = re.sub(r'^Summary', '', llm_answ, flags=re.IGNORECASE)
            print(f"Group: {gname}, query:{len(query)}, summary: {len(llm_answ)}")
            # 保证group_prompt是字符串
            tb_df['group_prompt'] = tb_df['group_prompt'].astype('str')
            tb_df.loc[tb_df['group_name']==gname,'group_prompt'] = str(llm_answ)
            pj_desc.append(llm_answ)
            
        query = tmpl.format(chat_lang=self.lang, limit_length=limit_length, db_info='\n'.join(pj_desc))
        llm_answ = await self.llm.ask_llm(query, '')
        llm_answ = re.sub(r'^Summary', '', llm_answ, flags=re.IGNORECASE)
        pj_df['db_desc'] = str(llm_answ)
        # 1 word = 5 characters
        if len(''.join(pj_desc)) > limit_length*5:
            print(f"too long to input for summary: {len(''.join(pj_desc))}")
            pj_desc[0] = f'This database contains a total of {tb_count} tables, grouped according to their functionality. The schema information for the tables is as follows:' 
            pj_df['db_prompt'] = '\n'.join(pj_desc)
        else:
            sort_df = tb_df.sort_values(by='group_name')
            prompts =[f'This database contains a total of {tb_count} tables, grouped according to their functionality. The schema information for the tables is as follows:' ]
            md = sort_df[['table_name','tb_desc','tb_prompt','group_name']].to_markdown(index=False)
            prompts.append(md)
            query = tmpl.format(chat_lang=self.lang,limit_length=limit_length, db_info='\n'.join(prompts))
            llm_answ = await self.llm.ask_llm(query, '')
            llm_answ = re.sub(r'^Summary', '', llm_answ, flags=re.IGNORECASE)
            pj_df['db_prompt'] = str(llm_answ)
            print(f"Database summary: {len(llm_answ)}")

        writer = pd.ExcelWriter(f'{meta_xls}')
        for tb_name, df in meta_dfs.items():
            df.to_excel(writer, sheet_name=f'{tb_name}', index=False)
        writer.close()

    def output_schma(self,xls_name):

        writer = pd.ExcelWriter(f'{xls_name}')
        for tb_name, df in self.scma_dfs.items():
            tdict = df.to_dict(orient='records')
            print(f"Table: {tb_name}, Rows: {len(tdict)}")
            df.to_excel(writer, sheet_name=f'{tb_name}', index=False)
        writer.close()
        return xls_name

# Example usage
if __name__ == '__main__':

    from zebura_core.placeholder import make_dbServer

    s_name = 'Postgres1'
    dbServer = make_dbServer(s_name)
    dbServer['db_name'] = 'olist'
   
    mg = ScmaGen(dbServer,'english')
    #mg.gen_db_info()
   
    # 创建存放文件的目录
    out_path=f'{const.S_TRAINING_PATH}/{dbServer["db_name"]}'
    wk_dir = os.getcwd()
    directory = os.path.join(wk_dir,out_path)
    if not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)
    xls_name = os.path.join(directory, f'{const.S_METADATA_FILE}')  
    #mg.output_schma(xls_name)
    print(f"Metadata generated to {directory}")
    #asyncio.run(mg.define_groups_tags(xls_name))
    #asyncio.run(mg.tb_enhance(xls_name))
    #asyncio.run(mg.define_hyperfield(xls_name))
    asyncio.run(mg.field_enhance(xls_name))

