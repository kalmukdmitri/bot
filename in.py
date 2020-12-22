import pandas as pd
import gspread
import requests
import datetime
import json
from doc_token import get_tokens
import mysql.connector as mysql
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
all_token = get_tokens()

def query_df(qry, token):
    devDB  = token
    cnx = mysql.connect(**devDB)
    cursor = cnx.cursor()
    cursor.execute(qry)
    resula = [i for i in cursor]
    field_names = [i[0] for i in cursor.description]
    cursor.close()
    cnx.close()
    db_data_df = pd.DataFrame(resula,
                           columns = field_names)
    return db_data_df

def wf_reg_bot():
    r_dt = datetime.datetime.today()
    t_dt = datetime.datetime(r_dt.year,r_dt.month,r_dt.day)
    date1 = t_dt - datetime.datetime(1970, 1, 1) - datetime.timedelta(days=1)
    date2 = t_dt - datetime.datetime(1970, 1, 1)
    date1_s = str(int(date1.total_seconds()))
    date2_s = str(int(date2.total_seconds()))
    query_compaines = f"""
    SELECT 
        users.phone,
        phone_status,
        users.email,
        email_status,
        create_date,
        companies.name
    FROM `users`

    left join `companies` on companies.user_id = users.user_id
    WHERE create_date > {date1_s}
    and create_date < {date2_s}"""
    new_regs  = query_df(query_compaines,all_token['wf_base'])

    goods = []
    bad  =[]
    for i in new_regs.itertuples():
        if i.phone_status == 1 or i.email_status == 1:
            phn_sts = "ДА" if i.phone_status == 1 else "НЕТ"
            email_sts = "ДА"  if i.email_status == 1 else "НЕТ"
            phone_data = 'Телефон - '+ i.phone+f" {phn_sts}"
            email_data = '; Email - ' + i.email+' '+str(email_sts)
            date_data = '\nДата создания: '+str(datetime.datetime.fromtimestamp(i.create_date))
            cmp_data = '; Компания: ' + i.name
            goods.append(phone_data + email_data+date_data+cmp_data)

        else:
            phn_sts = "ДА" if i.phone_status == 1 else "НЕТ"
            email_sts = "ДА"  if i.email_status == 1 else "НЕТ"
            phone_data = 'Телефон - '+ i.phone
            email_data = '; Email - ' + i.email
            date_data = '\nДата создания: '+str(datetime.datetime.fromtimestamp(i.create_date))
            cmp_data = '; Компания: ' + i.name
            bad.append(phone_data + email_data+date_data+cmp_data)

    rep_dt = str((datetime.datetime.today()- datetime.timedelta(days=1)).date())
    mess = f'Резулататы по регистрациям за {rep_dt}\n'
    if len(new_regs) == 0:
        mess+= 'Нет новых регистраций'
    else:
        mess+= 'Новые уcпешные регистрации:\n\n'
        for i in goods:
            mess+= i+'\n\n'
        mess+= 'Новые неподверждённые регистрации:\n\n'
        for i in bad:
            mess+= i+'\n\n'
    chats = [247391252, -358464301]            
    token_local = "1416074989:AAECtHYON681siUb5S1bzuMHKnLUI-qnb9M"
    method = "sendMessage"
    
    url = "https://api.telegram.org/bot{token}/{method}".format(token=token_local, method=method)
    for i in chats:
        data = {"chat_id": i, "text": mess}
        requests.post(url, data=data)
all_token = get_tokens()

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly',
             'https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name('kalmuktech-5b35a5c2c8ec.json', SCOPES)
gc = gspread.authorize(credentials)

def GoogleListUpdate(doc_Name,list_Name,dataframe, offsetright=1, offsetdown=1):
    """this fucn inserts Dataframe to the set google sheets"""
    edex = len(list(dataframe.axes[0]))
    eter = len(list(dataframe.axes[1]))
    print(edex,eter)
    sh = gc.open(doc_Name)
    worksheet1 = sh.worksheet(list_Name)
    for dexlet in range(0,eter):
        i = 0
        cell_list = worksheet1.range(offsetdown,dexlet+offsetright,edex+offsetdown-1,dexlet+offsetright)
        for cell in cell_list:
            insert_data = int(dataframe[dexlet][i]) if str(dataframe[dexlet][i]).isdigit() else dataframe[dexlet][i]
            cell.value = insert_data
            i+=1  
        worksheet1.update_cells(cell_list)

def GoogleListextract(doc_Name,list_Name):
    """this fucn exctract list of list of google sheets"""
    sh = gc.open(doc_Name)
    worksheet1 = sh.worksheet(list_Name)
    values_list = worksheet1.row_values(1)
    end_ls = []
    for i in range(1,len(values_list)+1):
        end_ls.append(list(worksheet1.col_values(i)))

    return end_ls

def modify_log(old_log, new_log):
    """this merges data form previos log and new logs"""
    heads_old = { head[0]:num for num,head in enumerate(old_log)}
    heads_new  = { head[0]:num for num,head in enumerate(new_log)}
    
    for i in old_log:
        if i[0] not in heads_new:
            i.insert(1,0)

    for i in new_log:
        if i[0] in heads_old:
            old_row = heads_old[i[0]]
            old_log[old_row].insert(1,i[1])
        else:
            old_log.append(i)
    return pd.DataFrame(old_log).transpose()
def query_df(qry, token):
    devDB  = token
    cnx = mysql.connect(**devDB)
    cursor = cnx.cursor()
    cursor.execute(qry)
    resula = [i for i in cursor]
    field_names = [i[0] for i in cursor.description]
    cursor.close()
    cnx.close()
    db_data_df = pd.DataFrame(resula,
                           columns = field_names)
    return db_data_df

def table_g_updater():
    r_dt = datetime.datetime.today()
    t_dt = datetime.datetime(r_dt.year,r_dt.month,r_dt.day)
    date1 = t_dt - datetime.datetime(1970, 1, 1) - datetime.timedelta(days=1)
    date2 = t_dt - datetime.datetime(1970, 1, 1)
    date1_s = str(int(date1.total_seconds()))
    date2_s = str(int(date2.total_seconds()))

    query_compaines = f"""
    SELECT 
        users.phone,
        phone_status,
        users.email,
        email_status,
        create_date,
        companies.name
    FROM `users`

    left join `companies` on companies.user_id = users.user_id
    WHERE create_date > {date1_s}
    and create_date < {date2_s}"""
    new_regs  = query_df(query_compaines,all_token['wf_base'])
    log_file = [['Телефон'],['Статус Телефона'],['Почта'],['Статус почты'],['Дата'],['Имя компании']]
    for i in new_regs.itertuples():

        log_file[0].append(i.phone)
        if i.phone_status == 1:
            log_file[1].append('Успешный')
        elif i.phone_status == 2:
            log_file[1].append('Не подтверждённый')
        log_file[2].append(i.email)
        if i.email_status == 1:
            log_file[3].append('Успешный')
        elif i.email_status == 2:
            log_file[3].append('Не подтверждённый')

        log_file[4].append(str(datetime.datetime.utcfromtimestamp(i.create_date)))
        log_file[5].append(i.name)

    old_log = GoogleListextract('WF phone logs','log')
    new_log = log_file
    heads_old = { head[0]:num for num,head in enumerate(old_log)}
    heads_new  = { head[0]:num for num,head in enumerate(new_log)}

    eds = []
    for i in heads_old:
        row = []
        row.append(i)
        row.extend(new_log[heads_new[i]][1:])
        row.extend(old_log[heads_old[i]][1:])
        eds.append(row)

    pd_end = pd.DataFrame(eds).transpose()
    GoogleListUpdate('WF phone logs','log',pd_end)
    

wf_reg_bot()
table_g_updater()