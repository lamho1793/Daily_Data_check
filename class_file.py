import json
from platform import processor
import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
import numpy as np
from datetime import date, timedelta
import sys

todaydate = date.today() 
subtractdays=1
yesterdaydat= todaydate-timedelta(days=subtractdays)

class store_status:

    global global_url
    global no

#Function sending the request to login and get the cookies
#username and password could be edited in payload
#save and return both cookies and session in a list
    def get_cookies():
        url1 = f'''{global_url}/dev/?realm=realm{no}''' 

        headers ={
        "user-agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:101.0) Gecko/20100101 Firefox/101.0"
        }
        payload = {
        "username":"itboss",
        "password":"Itboss1@3"
        }
        s = requests.session()
        r1= s.get(url=url1,headers=headers)
        url_login=bs(r1.content,'lxml').find('form').get('action')
        response = s.post(url=url_login,data=payload,headers=headers)
        r=s.cookies
        cookies = requests.utils.dict_from_cookiejar(r)
        cookie_string = "; ".join([str(x)+"="+str(y) for x,y in cookies.items()])
        return([s,cookie_string])

    def getprocessors_status_history(cookies,input_data):
        processor_id_dictionary={}
        task_id_dictionary={}
        status={}
        data = np.asarray(input_data)
        processors_status_detial=[]
        headers = {
            'Cookie': cookies,
            }
        processorgpsid=data[:,0]
        for i in range(len(processorgpsid)):
            task_id_dictionary[data[i,0]]=data[i,1]
            if data[i,3]=='inspurinsight':
                processgpflowcluster=82
            elif data[i,3]=='inspurinsight02':
                processgpflowcluster=83
            elif data[i,3]=='inspurinsight03':
                processgpflowcluster=84
            temp={}
            try:
                url=f'''{global_url}:{processgpflowcluster}/nifi-api/flow/process-groups/{processorgpsid[i]}'''
                r=requests.request("GET",url,headers=headers).text
                Json_r=json.loads(r)
                Start_Count=0
                Stop_Count=0
                Disable_Count=0
                status[processorgpsid[i]]={}
                for y in Json_r['processGroupFlow']['flow']['processors']:
                    if y['status']['runStatus']=='Running':
                        Start_Count+=1
                        status[processorgpsid[i]][y['id']]=1
                        processor_id_dictionary[y['id']]=y['component']['name']
                    elif y['status']['runStatus']=='Stopped':
                        Stop_Count+=1
                        status[processorgpsid[i]][y['id']]=0
                        processor_id_dictionary[y['id']]=y['component']['name']
                    elif y['status']['runStatus']=='Disabled':
                        Disable_Count+=1
                        status[processorgpsid[i]][y['id']]=2
                        processor_id_dictionary[y['id']]=y['component']['name']
    
                temp['Started']=Start_Count
                temp['Stoped']=Stop_Count
                temp['Dsiabled']=Disable_Count

                processors_status_detial.append(temp)
            except:
                print(data[i,0])
                continue

            processors_status=pd.DataFrame(processors_status_detial, index=list(range(len(processors_status_detial))))

        #df = pd.read_csv("itboss-02-prod-tasks_12072022.csv")
        input_data['Started']=processors_status['Started']
        input_data['Stoped']=processors_status['Stoped']
        input_data['Dsiabled']=processors_status['Dsiabled']
        input_data.to_csv(f'''{todaydate}_tasks_status_Checklist.csv''', index=False)

        with open(f'''C:/Users/PT4358/Desktop/Processors_Status_Alert/{todaydate}_processors_status.json''', 'w+') as w:
            w.write(json.dumps(status))
            w.close()

        with open(f'''C:/Users/PT4358/Desktop/Processors_Status_Alert/{todaydate}_processors_dictionary.json''', 'w+', encoding='utf8') as w:
            w.write(json.dumps(processor_id_dictionary,ensure_ascii=False))
            w.close()    

        with open(f'''C:/Users/PT4358/Desktop/Processors_Status_Alert/{todaydate}_tasks_dictionary.json''', 'w+') as w:
            w.write(json.dumps(task_id_dictionary))
            w.close()   

        return status

# def store_task_processor_status(status):
#     processor_status_code=[]
#     processor_status_v2={}
#     for x in status:
#         temp=list(status[x].values())
#         temp_2=np.asarray(temp)
#         processor_status_v2[x]=temp
#         status_number=''.join(map(str, temp))
#         processor_status_code.append(status_number)

#     processors_status=pd.DataFrame(processor_status_code, index=list(range(len(processor_status_code))))
#     processors_status_v2=pd.DataFrame(processor_status_code, index=status.keys())
#     print(processors_status_v2)

#     processors_status_v2.to_csv("14072022_processors_status_v2.csv", index=status.keys())

#     df = pd.read_csv("itboss-02-prod-tasks_13072022.csv")
#     df['Today status code']=processors_status
#     df.to_csv("itboss-02-prod-tasks_13072022.csv", index=False)

#     with open('14072022_processors_status_v2.json', 'w+') as w:
#         w.write(json.dumps(processor_status_v2))
#         w.close()
    
#     with open('14072022_processors_status_v3.json', 'w+') as w:
#         w.write(json.dumps(processor_status_code))
#         w.close()

#     return processor_status_code




# store_task_processor_status(status)

class valid:

    def validation():

        def  f(x):
            match x:
                case 1:
                    return 'Started'
                case 0:
                    return 'Stopped'
                case 2:
                    return 'Disabled'

        temp = open(f'''C:/Users/PT4358/Desktop/Processors_Status_Alert/{todaydate}_processors_status.json''')
        today_statuse=json.load(temp)
        temp.close()

        temp = open(f'''C:/Users/PT4358/Desktop/Processors_Status_Alert/Correct_processors_status.json''')
        Correct_status=json.load(temp)
        temp.close()

        temp = open(f'''C:/Users/PT4358/Desktop/Processors_Status_Alert/Correct_processors_status.json''')
        yesterday_statuse=json.load(temp)
        temp.close()

        temp = open(f'''C:/Users/PT4358/Desktop/Processors_Status_Alert/{todaydate}_processors_dictionary.json''',encoding='utf8')
        new_processor_id_dictionary=json.load(temp)
        temp.close()

        temp = open(f'''C:/Users/PT4358/Desktop/Processors_Status_Alert/Proper_processors_dictionary.json''',encoding='utf8')
        old_processor_id_dictionary=json.load(temp)
        temp.close()

        temp = open(f'''C:/Users/PT4358/Desktop/Processors_Status_Alert/Proper_tasks_dictionary.json''')
        old_task_id_dictionary=json.load(temp)
        temp.close()

        temp = open(f'''C:/Users/PT4358/Desktop/Processors_Status_Alert/{todaydate}_tasks_dictionary.json''')
        new_task_id_dictionary=json.load(temp)
        temp.close()

        Alt_status_processors={}

        if old_task_id_dictionary!=new_processor_id_dictionary:
            old_task=set(old_task_id_dictionary.keys())
            new_task=set(new_task_id_dictionary.keys())
            union_task=old_task.union(new_task)
            del_unqiue_tasks=union_task-new_task
            new_unique_tasks=union_task-old_task
            
            del_tasks=[]
            new_tasks=[]
            for x in del_unqiue_tasks:
                del_tasks.append(old_task_id_dictionary[x])
            for x in new_unique_tasks:
                new_tasks.append(new_task_id_dictionary[x])          

            del_processsors={}
            new_create_processsors={}            

        for x in today_statuse:

            processor_id=list(today_statuse[x].keys())

            if x in old_task_id_dictionary:
                if today_statuse[x].keys()!=yesterday_statuse[x].keys():
                    old_processsors=set(yesterday_statuse[x].keys())
                    new_processsors=set(today_statuse[x].keys())
                    union_processors= old_processsors.union(new_processsors)
                    del_processsors_set=union_processors-new_processsors
                    new_create_processsors_set=union_processors-old_processsors
                    
                    del_processsors[old_task_id_dictionary[x]]=[]
                    new_create_processsors[old_task_id_dictionary[x]]=[]
                    for y in del_processsors_set:
                        del_processsors[old_task_id_dictionary[x]].append(old_processor_id_dictionary[y])
                        del old_processor_id_dictionary[y]
                        del yesterday_statuse[x][y]
                    for y in new_create_processsors_set:
                        new_create_processsors[old_task_id_dictionary[x]].append(new_processor_id_dictionary[y])
                        old_processor_id_dictionary[y]=new_processor_id_dictionary[y]
                        yesterday_statuse[x][y]=today_statuse[x][y]                      
        
            for y in processor_id:
                
                try:
                    if today_statuse[x][y]!=yesterday_statuse[x][y]:
                    
                        if old_task_id_dictionary[x] not in Alt_status_processors.keys():
                            Alt_status_processors[old_task_id_dictionary[x]]={}

                        Alt_status_processors[old_task_id_dictionary[x]][new_processor_id_dictionary[y]]=[]
                        Alt_status_processors[old_task_id_dictionary[x]][new_processor_id_dictionary[y]].append(f(yesterday_statuse[x][y]))
                        Alt_status_processors[old_task_id_dictionary[x]][new_processor_id_dictionary[y]].append(f(today_statuse[x][y]))
                
                except:
                    continue

        with open(f'''C:/Users/PT4358/Desktop/Processors_Status_Alert/{todaydate}_Validation.json''', 'w+', encoding='utf-8') as w:
            w.write(json.dumps(Alt_status_processors,ensure_ascii=False))
            w.close()

        with open(f'''C:/Users/PT4358/Desktop/Processors_Status_Alert/Correct_processors_status.json''', 'w+') as w:
            w.write(json.dumps(yesterday_statuse))
            w.close()

        with open(f'''C:/Users/PT4358/Desktop/Processors_Status_Alert/Proper_processors_dictionary.json''', 'w+') as w:
            w.write(json.dumps(old_processor_id_dictionary))
            w.close() 
    
        return [Alt_status_processors,del_tasks,new_tasks,del_processsors,new_create_processsors]
