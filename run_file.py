from class_file import store_status
from class_file import valid
import class_file
import Alert_Bot as bot
import timeit
import pymysql
import pandas as pd
import xlsxwriter
from datetime import date, timedelta
import os
import sys
import time
import shutil

todaydate = date.today() 
subtractdays=1
yesterdaydat= todaydate-timedelta(days=subtractdays)
Dbfyesterday= todaydate-timedelta(days=subtractdays+4)

# def write(data,sql_data):
#    #jsonformat=json.loads(data)
#    workbook = xlsxwriter.Workbook(f'''C:/Users/PT4358/Desktop/Processors_Status_Alert/{todaydate}_Processors_Switiched_Status_report.xlsx''')
#    w = workbook.add_worksheet()
#    bold = workbook.add_format({'bold': 1})
#    print ('######################START WRITIE####################################')
#    w.write('A1', 'taskName', bold)
#    w.write('B1', 'Cluster_Name', bold)
#    w.write('C1', 'Corresponding_Problem_Processors', bold)
#    w.write('D1', 'YesterdayStatus', bold)
#    w.write('E1', 'TodayStatus', bold)
#    row = 1
#    col = 0

#    for x in (data):
#      # Convert the date string into a datetime object.
#      w.write_string  (row, col, x)
#      w.write_string  (row, col+1, sql_data[sql_data['name']==x]['cluster_name'].values[0])
#      for y in data[x]:
#         w.write_string  (row, col+2, y)
#         w.write_string  (row, col+3,     data[x][y][0])
#         w.write_string  (row, col+4,     data[x][y][1])
#         row += 1
  
#    workbook.close()
#    print('##########################FINISED WRITIE################################')


def get_relation():
    input_id=[]
    sql="""select id, name, processor_id, cluster_name from indata.bde_nifi_task where (cluster_name='inspurinsight' or cluster_name='inspurinsight02' or cluster_name='inspurinsight03') and user_id='itboss-realm1234' and groupname=3;"""
    connection = pymysql.connect(
                             host='10.250.50.139',
                             port=3306,
                             user='BDO',
                             password='Cmhk1@345',
                             cursorclass=pymysql.cursors.DictCursor)
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(sql)
            result = cursor.fetchall()
            data=pd.DataFrame.from_dict(result)        
    
    cursor.close()
    return data

if __name__ == "__main__":

    print(sys.executable)
    class_file.global_url="http://10.250.50.19"
    class_file.no="1234"
    start = timeit.default_timer()
    sql_data=get_relation()
    cookie_test=store_status.get_cookies()
    status=store_status.getprocessors_status_history(cookie_test[1],sql_data)
    stop = timeit.default_timer()
    print('Time: ', (stop - start)/60)

    start = timeit.default_timer()

    try:
        # open(f'''C:/Users/PT4358/Desktop/Processors_Status_Alert/{yesterdaydat}_processors_status.json''')
        result=valid.validation() 

        if not result[0]: 
            print('Processors Status Normal')
            if result[1] or result[2] or  result[3] or result[4]:
                if os.path.exists(f'''C:/Users/PT4358/Desktop/Processors_Status_Alert/{todaydate}_processors_dictionary.json'''):
                    shutil.copy(f'''C:/Users/PT4358/Desktop/Processors_Status_Alert/{todaydate}_processors_dictionary.json''', f'''C:/Users/PT4358/Desktop/Processors_Status_Alert/'Proper_processors_dictionary.json''')
                    # os.rename(f'''{todaydate}_processors_status.json''','Proper_processors_dictionary.json') 

                if os.path.exists(f'''C:/Users/PT4358/Desktop/Processors_Status_Alert/{todaydate}_tasks_dictionary.json'''):
                    shutil.copy(f'''C:/Users/PT4358/Desktop/Processors_Status_Alert/{todaydate}_tasks_dictionary.json''', f'''C:/Users/PT4358/Desktop/Processors_Status_Alert/'Proper_tasks_dictionary.json''')
                    # os.rename(f'''{todaydate}_tasks_dictionary.json''','Proper_tasks_dictionary.json') 

                if os.path.exists(f'''C:/Users/PT4358/Desktop/Processors_Status_Alert/{todaydate}_processors_status.json'''):
                    print(True)
                    shutil.copy(f'''C:/Users/PT4358/Desktop/Processors_Status_Alert/{todaydate}_processors_status.json''', f'''C:/Users/PT4358/Desktop/Processors_Status_Alert/Correct_processors_status.json''')
                    # os.rename(f'''{todaydate}_processors_status.json''','Correct_processors_status') 
                else:
                    print(False)
        else:
            bot.check_json_status(result[0])
        
        if result[1] or result[2]:
            bot.check_json_tasks(result[1:3])

        if result[3] or result[4]:
            bot.check_json_Processors(result[3:5])

        stop = timeit.default_timer()
    except:
        stop = timeit.default_timer()
        print("No pervious day processors status summary")


    if os.path.exists(f'''C:/Users/PT4358/Desktop/Processors_Status_Alert/{Dbfyesterday}_processors_dictionary.json'''):
        os.remove(f'''C:/Users/PT4358/Desktop/Processors_Status_Alert/{Dbfyesterday}_processors_dictionary.json''')

    if os.path.exists(f'''C:/Users/PT4358/Desktop/Processors_Status_Alert/{Dbfyesterday}_processors_status.json'''):
        os.remove(f'''C:/Users/PT4358/Desktop/Processors_Status_Alert/{Dbfyesterday}_processors_status.json''')
    
    if os.path.exists(f'''C:/Users/PT4358/Desktop/Processors_Status_Alert/{Dbfyesterday}_Processors_Switiched_Status_report.xlsx'''):
        os.remove(f'''C:/Users/PT4358/Desktop/Processors_Status_Alert/{Dbfyesterday}_Processors_Switiched_Status_report.xlsx''')

    if os.path.exists(f'''C:/Users/PT4358/Desktop/Processors_Status_Alert/{Dbfyesterday}_tasks_dictionary.json'''):
        os.remove(f'''C:/Users/PT4358/Desktop/Processors_Status_Alert/{Dbfyesterday}_tasks_dictionary.json''')

    if os.path.exists(f'''C:/Users/PT4358/Desktop/Processors_Status_Alert/{Dbfyesterday}_tasks_status_Checklist.csv'''):
        os.remove(f'''C:/Users/PT4358/Desktop/Processors_Status_Alert/{Dbfyesterday}_tasks_status_Checklist.csv''') 

    if os.path.exists(f'''C:/Users/PT4358/Desktop/Processors_Status_Alert/{Dbfyesterday}_Validation.json'''):
        os.remove(f'''C:/Users/PT4358/Desktop/Processors_Status_Alert/{Dbfyesterday}_Validation.json''') 
        

    print('Time: ', stop - start)
    time.sleep(60)
