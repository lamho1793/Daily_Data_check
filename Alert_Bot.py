import requests,json,sys
from datetime import date

todaydate = date.today() 

proxies = {
   'http': 'http://172.19.1.21:6588',
   'https': 'http://172.19.1.21:6588',
}

receiver=['P3375','P4370','P6479','P4894','P5274','P5892','P5939','P5971','P6534','P6587','P6437','P6727','P6750','P6486','PT4358']
#receiver=['PT4358']

def gettenant_access_token():
    tokenurl="https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    headers={"Content-Type":"application/json"}
    data={
        "app_id":"cli_a1f5f330a5f9d00e",
        "app_secret":"QFzhtAJwEdPiUGF9c4tpKeub2TEKI3x8"
    }
    request=requests.post(url=tokenurl,headers=headers,json=data,proxies=proxies)
    response=json.loads(request.content)['tenant_access_token']
    return response

def sendmes(chat_id,tenant_access_token,title,messages):
        sendurl="https://open.feishu.cn/open-apis/message/v4/batch_send?receive_id_type=user_id"
        headers={"Authorization":"Bearer %s"%tenant_access_token,"Content-Type":"application/json"}
        data = {
                "user_ids": [chat_id],
                "msg_type": "post",
                "content": {
                    "post":{
                        "zh_cn": {
                            "title": title,
                            "content": messages
                        }
                }
            }
        }
        request=requests.post(url=sendurl,headers=headers,json=data,proxies=proxies)
        print(request.content)

def check_json_status(data):

    tenant_access_token = gettenant_access_token()
    for x in data:
        messages=[]
        for y_1 in data[x]:
            messages.append([{
                                    "tag": "text",
                                    "text": f'''{y_1}: ({data[x][y_1][0]}) --> ({data[x][y_1][1]})'''
                                }])
                                
        for y_2 in receiver:
            # first parameter:
            sendmes(y_2, tenant_access_token,f'''Check Important nifi task: {x}''', messages)

def check_json_Processors(data):

    tenant_access_token = gettenant_access_token()
    nl='\n'
    messages=[]
    for x in data[0]:
        if data[0][x]:
            messages.append([{
                        "tag": "text",
                        "text": f"Removed Processors: {nl}{nl.join(data[0][x])}"
                 }])

        if data[1][x]:
            messages.append([{
                        "tag": "text",
                        "text": f"New Processors: {nl}{nl.join(data[1][x])}"
                 }])                 
                                
        for y_2 in receiver:
            # first parameter:
            sendmes(y_2, tenant_access_token,f'''Check Removed & New Processors in Important nifi task:{x}''', messages)

def dash():
    tenant_access_token = gettenant_access_token()
    for y_2 in receiver:
        # first parameter:
        sendmes(y_2, tenant_access_token,f'''----------------------------------{todaydate}----------------------------------''',[])

def check_json_tasks(data):

    tenant_access_token = gettenant_access_token()
    nl='\n'
    messages_1=[]
    messages_2=[]
    if data[0]:
        messages_1.append([{
                        "tag": "text",
                        "text": f"{nl}{nl.join(data[0])}"
                 }])
        for y_2 in receiver:
        # first parameter:
            sendmes(y_2, tenant_access_token,'Check Removed nifi Tasks', messages_1)
        
    if data[1]:
        messages_2.append([{
                        "tag": "text",
                        "text": f"{nl}{nl.join(data[1])}"
                 }])                 
                                
        for y_2 in receiver:
        # first parameter:
            sendmes(y_2, tenant_access_token,'Check New nifi Tasks', messages_2)

