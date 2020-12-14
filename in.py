import pandas as pd
import gspread
import requests
import datetime
import json
from doc_token import get_tokens
import mysql.connector as mysql
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
token = get_tokens()

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

    query_compaines = f'''
    SELECT 
        phone,
        phone_status,
        create_date 
    FROM `users`
    WHERE create_date > {date1_s}
    and create_date < {date2_s}'''
    new_regs  = query_df(query_compaines,token['wf_base'])
    log_file = [['Телефон'],['Статус'],['Дата']]
    for i in new_regs.itertuples():
        log_file[0].append(i.phone)
        if i.phone_status == 1:
            log_file[1].append('Успешный')
        elif i.phone_status == 2:
            log_file[1].append('Не подтверждённый')
        log_file[2].append(str(datetime.datetime.utcfromtimestamp(i.create_date)))

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
    
table_g_updater()

def wf_reg_bot():
    r_dt = datetime.datetime.today()
    t_dt = datetime.datetime(r_dt.year,r_dt.month,r_dt.day)
    date1 = t_dt - datetime.datetime(1970, 1, 1) - datetime.timedelta(days=1)
    date2 = t_dt - datetime.datetime(1970, 1, 1)
    date1_s = str(int(date1.total_seconds()))
    date2_s = str(int(date2.total_seconds()))

    query_compaines = f'''
    SELECT 
        phone,
        phone_status,
        create_date 
    FROM `users`
    WHERE create_date > {date1_s}
    and create_date < {date2_s}'''
    new_regs  = query_df(query_compaines,token['wf_base'])
    nul_phones = []
    good_phones  =[]
    for i in new_regs.itertuples():
        if i.phone_status == 1:
            good_phones.append(i.phone)
        else:
            nul_phones.append(i.phone)

    mess = f'Резулататы по регистрациям за {str(t_dt.date())}\n'
    if len(new_regs) == 0:
        mess+= 'Нет новых регистраций'
    else:
        mess+= 'Новые уcпешные регистрации:\n'
        for i in good_phones:
            mess+= i+'\n'
        mess+= 'Новые неподверждённые регистрации:\n'
        for i in nul_phones:
            mess+= i+'\n'
#     chats = [247391252, 482876050]            
    chats = [247391252]
    token = "1416074989:AAECtHYON681siUb5S1bzuMHKnLUI-qnb9M"
    method = "sendMessage"
    
    url = "https://api.telegram.org/bot{token}/{method}".format(token=token, method=method)
    for i in chats:
        data = {"chat_id": i, "text": mess}
        requests.post(url, data=data)


wf_reg_bot()

SCOPES = ['https://www.googleapis.com/auth/drive',
        'https://www.googleapis.com/auth/documents']

credentials = ServiceAccountCredentials.from_json_keyfile_name('kalmuktech-5b35a5c2c8ec.json', SCOPES)
service = build('docs', 'v1', credentials=credentials)

class callibri():
    base = 'https://api.callibri.ru/'
    def __init__(self, token, 
                user_email = 'user_email=kalmukdmitri@gmail.com',
                 site_id = 'site_id=37222'):
        self.token = token

        self.user_email = user_email
        self.site_id= site_id
    def get_stats(self, date1,date2):
        date1= date1.strftime("%d.%m.%Y")
        date2 = date2.strftime("%d.%m.%Y")
        request_url  = f"{callibri.base}site_get_statistics?{self.token}&{self.user_email}&{self.site_id}&date1={date1}&date2={date2}"
        print(request_url)
        answer = requests.get(request_url)
        results = json.loads(answer.text)
        return results
    
def get_old_token(docname):
    """Intup: None
    Output: Old token"""

    googe_request = service.documents().get(documentId = docname).execute()
    token_str=googe_request['body']['content'][1]['paragraph']['elements'][0]['textRun']['content']
    doc_lenth = len(token_str)
    token = json.loads(token_str.strip().replace("'", '"'))
    return token,doc_lenth

def write_new_token(docname, token, doc_lenth):
    requests = [
        {'deleteContentRange': {
            'range' : {
                "startIndex": 1,
    "endIndex": doc_lenth
            }
        }},
        {'insertText': {
                'location': {
                    'index': 1,
                },
                'text': str(token)
            }
        }
    ]
    result = service.documents().batchUpdate(
        documentId=docname, body={'requests': requests}).execute()

def get_new_token(docname):
    """Intup: None
    Process: write new token instead of old one
    Output: New token """

    old_token,doc_lenth = get_old_token(docname)
    url = 'https://officeicapru.amocrm.ru/oauth2/access_token'
    data = json.dumps({
    "client_id": "e8b09b6a-3f20-43fa-9e63-7e045cb5dbeb",
    "client_secret": "IO61BEABH48e5VcQzL7ivtcCGSKg6NXnv8zRVumsiC5EbGQegV9ox0e5CJPVjMop",
    "grant_type": "refresh_token",
    'redirect_uri':"https://officeicapru.amocrm.ru/",
    "refresh_token": old_token['refresh_token']
                    })


    token = json.loads(requests.post(url, headers = {"Content-Type":"application/json"},data=data).text)
    write_new_token(docname,token,doc_lenth)

    return token

class get_AMO:
    m_url = "https://officeicapru.amocrm.ru/api/v2/"
    def __init__(self, token):
        self.headers = {
        'Authorization': f"Bearer {token}",
        "Content-Type":"application/json"
        }
    def get_data(self, prm):
        url = f"{get_AMO.m_url}{prm}"
        print(url)
        reqv = requests.get(url, headers = self.headers)       
        return json.loads(reqv.text)

    def get_big_amo(self,params):
        i = True
        c = -1
        res = []
        while i:
            c+=1
            offset = c * 500
            params_url = f'{params}?limit_rows=500&limit_offset={offset}'
            result = self.get_data(params_url)
            if '_embedded' not in result:
                return res
            else:
                result = result['_embedded']['items']
            res.extend(result)
            len_res= len(result)
            if c == 100 or len_res < 500: 
                i = False
        return res
        
def call_hole():
    passwords = get_tokens()
    callibri_connect = callibri(token= passwords['callibri'])
    date2 = datetime.datetime.today().date()  - datetime.timedelta(days=1)
    date1  = date2 - datetime.timedelta(days=5)
    callibri_data = callibri_connect.get_stats(date1, date2)
    cal_ph = {i['phone']:(i['date'][:10]+' '+i['date'][11:19]) for i in callibri_data['channels_statistics'][0]['calls']}

    SCOPES = ['https://www.googleapis.com/auth/drive',
            'https://www.googleapis.com/auth/documents']

    credentials = ServiceAccountCredentials.from_json_keyfile_name('kalmuktech-5b35a5c2c8ec.json', SCOPES)
    service = build('docs', 'v1', credentials=credentials)

    current_token = get_new_token("1V1gX11RDYJf4ZVFCEOqp-kY5j6weApl_oEFkv2oZzW4")
    amo_connect = get_AMO(current_token['access_token'])
    r_dt = datetime.datetime.today()
    t_dt = datetime.datetime(r_dt.year,r_dt.month,r_dt.day)
    date1 = t_dt - datetime.datetime(1970, 1, 1) - datetime.timedelta(days=6)
    date2 = t_dt - datetime.datetime(1970, 1, 1) - datetime.timedelta(days=1)
    date1_s = str(int(date1.total_seconds()))
    date2_s = str(int(date2.total_seconds()))
    fresj_cnts = amo_connect.get_big_amo('contacts')

    import string
    def get_custom_phone(cstms , fld = 'Телефон'):
        for i in cstms:
            if 'name' in i and i['name'] == fld:
                phn  = ''
                for j in i['values'][0]['value']:
                    if j in string.digits:
                        phn += j
                return phn
    cnt_map = {get_custom_phone(i['custom_fields']) : i['id'] for i in fresj_cnts}
    matches = []
    for i in cal_ph:
        if i in cnt_map:
            pass
        else:
            matches.append(i)

    matches = []

    for i in cal_ph:
        if i in cnt_map:
            pass
        else:
            matches.append(i)

    losts = {}
    for i in matches:
        losts[i] = cal_ph[i]
    if len(matches) > 0:
        message = 'Контакты не попавшие в амо:\n'
        for i,e in losts.items():
            message += f'{i} созданый {e}\n'
    else:
        message = 'Нет пропавших контактов'
    chats = [247391252,482876050]
    token = "1461276547:AAECMSMOMW1Zah3IEXeAyGAsBVJD0ktM86E"
    method = "sendMessage"
    
    url = "https://api.telegram.org/bot{token}/{method}".format(token=token, method=method)
    for i in chats:
        data = {"chat_id": i, "text": message}
        requests.post(url, data=data)
        
call_hole()