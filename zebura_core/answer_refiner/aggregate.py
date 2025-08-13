# 综合所有解析和查库的信息，最终合成给用户的答案
class Aggregate:
    def __init__(self):
        pass

    # main func, gathering/combining all info into anawer
    def gathering(self, pipeline) ->dict:
        print(f"gathering: {pipeline}")
        resp = self.make_answ()
        resp['question'] = pipeline[-1].get('question','')      # 用户提问
        resp['status'] = 'failed'
        resp['type'] = 'error'
        if not isinstance(pipeline,list) or len(pipeline) == 0:
            resp['error'] = 'action line is empty'
            return resp
        
        steps_info = ''
        for log in pipeline:
            steps_info += f'{log["from"]}:{log["status"]} ->'
        steps_info = steps_info[:-2]
        resp['reasoning'] = steps_info      # 记录推理过程

        fromList = [log['from'] for log in pipeline]
        if 'sql4db' in fromList:
           # 步长-1, 反转，找到最后一个sql4db的位置
            reverse_indx = fromList[::-1].index('sql4db')
            indx = len(fromList) - reverse_indx - 1
            if pipeline[indx]['status'] == 'succ':
                resp['status'] = 'succ'
                resp['type'] = 'sql'
                resp['sql'] = pipeline[-1]['sql']            # 最后一次DB查询之前的SQL
                resp['sql_result'] = pipeline[indx]['msg']
            else:
                resp['error'] = pipeline[indx]['msg']
                resp['type'] = 'error'
        
        if 'nl2sql' in fromList:
            reverse_indx = fromList[::-1].index('nl2sql')
            indx = len(fromList) - reverse_indx - 1
            log = pipeline[indx]
            if log['type'] == 'chat':
                resp['chat'] = log['msg']
                resp['type'] = 'chat'
            if log['type'] == 'error':
                resp['type'] = 'error'
                resp['error'] = log['msg']
               
        if 'exploration' in fromList:
            reverse_indx = fromList[::-1].index('exploration')
            indx = len(fromList) - reverse_indx - 1
            log = pipeline[indx]
            if log['status'] == 'succ':
                resp['suggestion'] = log['msg']+log['note']
            else:
                resp['suggestion'] = log['msg']
        
        # sql 情况
        key_steps = ['nl2sql','sql4db','rewrite', 'sql_refine', 'sql_correct']
        msgList = []
        for log in pipeline[1:]:
            step = log['from']
            if step not in key_steps:
                continue
            if log['status'] == 'failed':
                tStr = f"{step}:{log['status']} \nquestion: {log.get('question','')}\nsql:{log.get('sql','')}\n"
                tStr+= f"{log.get('msg','')}\n{log.get('note','')}"
                msgList.append(tStr)
        resp['key_info'] =msgList
        return resp
    
    @staticmethod
    def make_answ():
        answ = {'status'    : 'succ',
                'question'  : '',               # 用户提问
                'reasoning': '',                # 推理过程
                'type': 'sql',                  # 当前状态类型, sql, chat, error
                'sql_result': '', 
                'sql'       :'',                # 最终SQL
                'key_info'  : [],               # 推理步骤中的关键信息
                'chat'      : '',               # chat的信息
                'error'     : '',               # 错误信息
                'suggestion': []                # data exploration， Union[str,list]
                }
        return answ
    
if __name__ == "__main__":
    answerer = Aggregate()
    answ = answerer.gathering([
        {'from': 'nl2sql', 'status': 'succ', 'msg': 'select * from table1', 'type': 'sql','question':'what is the table1','sql': ''},
        {'from': 'rewrite', 'status': 'succ', 'msg': 'select * from table1', 'type': '','question':'what is the table1','sql': ''},
        {'from': 'sql_refine', 'status': 'succ', 'msg': 'select * from table1', 'type': '','question':'what is the table1','sql': ''},
        {'from': 'sql4db', 'status': 'succ', 'msg': 'select * from table1', 'type': '','question':'what is the table1','sql': ''},
        {'from': 'polish', 'status': 'succ', 'msg': 'select * from table1', 'type': '','question':'what is the table1','sql': ''},
        {'from': 'sql4db', 'status': 'succ', 'msg': 'select * from table1', 'type': '','question':'what is the table1','sql': ''}
    ])
    print(answ)
