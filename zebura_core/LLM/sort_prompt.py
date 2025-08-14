# 整理prompt.txt, 按 task name 升序
import os
import re
def sort_prompt(prompt_file):
    if not os.path.exists(prompt_file):
        print(f"Prompt file {prompt_file} not found")
        return False
    print(f"Loading prompt from {prompt_file}")

    header = ''
    tasks = {}
    tList = []
    comments = []
    content = ''
    with open(prompt_file, "r", encoding='utf-8') as f:
        lines = f.readlines()
        for line in lines:
            if line.startswith("//"): # 注释
                comments.append(line)
                continue
            if line.startswith("<TASK:") or len(line.strip()) == 0:
                if len(header) == 0:
                    comments = [tstr.strip() for tstr in comments if len(tstr.strip()) != 0]
                    header = '\n'.join(comments)
                    comments = []
            if line.startswith("<TASK:"):
                task_name = line.split(":")[1].strip()
                task_name = re.sub(r'[^\w]', '', task_name)
                task_name = task_name.lower()
                tasks[task_name] = ""
                content = ""
            elif line.startswith("</TASK>"):
                comments = [tstr.strip() for tstr in comments if len(tstr.strip()) != 0]
                tasks[task_name] = {'content': content, 'comments': '\n'.join(comments)}  
                tList.append(task_name)
                comments = []
            else:
                content += line
    print(f"Total tasks: {len(tList)}")
    print(tList)

    if len(tList) != len(tasks.keys()):
        print(tasks.keys())
        print(f"Total tasks: {len(tList)}, tasks: {len(tasks.keys())}")
    
    new_prompt_file = prompt_file.replace('.txt', '_sorted.txt')
    tList = list(tasks.keys())
    tList.sort()
    with open(new_prompt_file, "w", encoding='utf-8') as f:
        f.writelines(header)
        f.write('\n// sorted by task name\n')
        f.write(f"// Tasklist: {tList}\n")
        f.write('////////////////////////////////////////\n\n')
        for name in tList:
            f.write(f"{tasks[name]['comments']}\n")
            f.write(f"<TASK:{name}>\n")
            f.write(tasks[name]['content'])
            f.write(f"</TASK>\n\n")
            
if __name__ == '__main__':
    sort_prompt('C:/something/talq/zebura_lit/zebura_core/LLM/prompt.txt')