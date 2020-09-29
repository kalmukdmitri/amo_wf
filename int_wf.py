import time
import requests
import json
import pandas
import os
import mysql.connector as mysql
import pandas as pd
from google.cloud import bigquery
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from doc_token import get_tokens

import datetime
x = datetime.datetime.today()
SCOPES = ['https://www.googleapis.com/auth/drive',
        'https://www.googleapis.com/auth/documents']
import sys

credentials = ServiceAccountCredentials.from_json_keyfile_name('kalmuktech-5b35a5c2c8ec.json', SCOPES)
service = build('docs', 'v1', credentials=credentials)
token = get_tokens()

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



#Точка отсчёта для продаваца - первая интегрированная сделка



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
    "client_id": "890911e6-74ae-4569-b79b-c0a054f0068d",
    "client_secret": "o7KnfRWwB5VxBM3c0gU49MD9qs7btt1CNiNZKCjPii3mx9wQHw8orygepLmgmNdu",
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
            result = self.get_data(params_url)['_embedded']['items']
            res.extend(result)
            len_res= len(result)
            if c == 100 or len_res < 500: 
                i = False
        return res
    def post_leady_data(self, data):
        url = f"https://officeicapru.amocrm.ru/api/v4/leads"
        print(url)

        data = json.dumps({'body':data})
        reqv = requests.post(url, headers = self.headers, data=data)
        print(reqv)
        print(json.loads(reqv.text))
        return json.loads(reqv.text)

    def post_data(self,prms, data):
        url = f"https://officeicapru.amocrm.ru/api/v4/{prms}"
        print(url)

        data = json.dumps( data)
        print(data)
        reqv = requests.post(url, headers = self.headers, data=data)       
        return json.loads(reqv.text)

    def get(self, url):
        reqv = requests.get(url, headers = self.headers)
        return reqv

    def patch(self, prms, data):
        url = f"https://officeicapru.amocrm.ru/api/v4/{prms}"
        print(url)

        data = json.dumps(data)
        print(data)
        reqv = requests.patch(url, headers = self.headers, data=data)       
        return json.loads(reqv.text)

current_token = get_new_token("1o0--ETQ4EqyoqkX8uM5Xik9CIL-MRiGInR6LCPCa-As")
amo_connect = get_AMO(current_token['access_token'])
dicts_amo = amo_connect.get_data("account?with=pipelines,custom_fields,users")

def pipiline_loc(pipelines):
    pipeline_dicts = {}
    for i in pipelines:

        pipeline_dicts[i] = [pipelines[i]['name']]
        statuse = {}

        for j in pipelines[i]['statuses']:
            statuse[j] = pipelines[i]['statuses'][j]['name']

        pipeline_dicts[i].append(statuse)
    return pipeline_dicts

pips = pipiline_loc(dicts_amo['_embedded']['pipelines'])


class doc_json:
    def __init__(self, docname):
        self.docname = docname
        self.data = self.get_content(docname)
        self.lenth = len(self.data)

    def get_content(self,docname):
        googe_request = service.documents().get(documentId = docname).execute()
        token_str=googe_request['body']['content'][1]['paragraph']['elements'][0]['textRun']['content']
        return token_str




    def write(self, data):
        requests = [
            {'deleteContentRange': {
                'range' : {
                    "startIndex": 1,
        "endIndex": self.lenth
                }
            }},
            {'insertText': {
                    'location': {
                        'index': 1,
                    },
                    'text': str(data)
                }
            }
        ]
        result = service.documents().batchUpdate(
        documentId=self.docname, body={'requests': requests}).execute()
#         done = False
#         tries = 20
#         while not done or tries >0:
#             try:
#                 result = service.documents().batchUpdate(
#                 documentId=self.docname, body={'requests': requests}).execute()
#                 done = True
#                 tries -= 1
#             except:
#                 print(sys.exc_info())


        self.lenth = len(str(data))+1

        self.data = data


base_json = doc_json('1lLDlyeeJ_ojNB8SfqQKG-Ufc_MG3kYJSa9J028RK_6Q')
json_tracker = json.loads(base_json.data.strip().replace("'", '"'))

cmp_tracker = json_tracker['i-cap']

deal_id = cmp_tracker['last_order']
# Получение данных по сделке id ключ в словаре
supplier_company_id = cmp_tracker['supplier_company_id']
get_last_order = f"""SELECT * FROM `deals`
where `supplier_company_id` = {supplier_company_id}
and deal_id >{deal_id}"""

deals_info = query_df(get_last_order,token['wf_base'])

if len(deals_info) > 0:

    #Узнаём товары в сделке 
    deals_ids = (', ').join([str(i) for i in deals_info['deal_id']])
    get_order_products = f"""SELECT deal_id, caption, cnt, price FROM `deal_good_offers`
    where deal_id in ( {deals_ids} )"""

    deals_products = query_df(get_order_products,token['wf_base'])

    deals_data = {}

    for i in deals_products.itertuples():
        if i.deal_id in deals_data:
            deals_data[i.deal_id].append(i)
        else:
            deals_data[i.deal_id] = [i]

    def create_amo_lead(deals_info,deals_products,json_tracker = cmp_tracker):
        products = f"Дата заказа {str(datetime.datetime.utcfromtimestamp(deals_info.dt_create))} \n"
        for i in deals_products[deals_info.deal_id]:
            products += f"{i.caption}, {i.cnt} шт, {i.price} р за шт. \n"

        lead = {
            'lead_id' : deals_info.deal_id,
            'cnt_id' : deals_info.consumer_profile_id,
            'data':{
                'name': f"Сделка из Workface #{deals_info.deal_id}",
                'account_id': 23428672,
                'pipeline_id': json_tracker['pipeline_id'],
                'status_id': json_tracker['funnels'][str(deals_info.status)],
                'custom_fields_values': [
                    {'field_id': 671251,
                    'field_name': 'Дополнение',
                    'field_code': None,
                    'field_type': 'textarea',
                    'values': [{'value': products}]}
                ],
                'sale': int(deals_info.total_price)
                    }
               }

        return lead

    leads = []
    for order in deals_info.itertuples():
        leads.append(create_amo_lead(order, deals_data))

    # Получаем данные по клиенту id ключ в словаре
    def create_amo_cnt(cnt):

        cnts = {
            'cnt_id' : cnt.company_id,
            'data':{'name': str(cnt.contact_person),
               'custom_fields_values': [{'field_id': 50597,
               'field_name': 'Телефон',
               'field_code': 'PHONE',
               'field_type': 'multitext',
               'values': [{'value': str(cnt.phone),
                 'enum_id': 79907,
                 'enum_code': 'WORK'}]},
              {'field_id': 50599,
               'field_name': 'Email',
               'field_code': 'EMAIL',
               'field_type': 'multitext',
               'values': [{'value': str(cnt.email), 
                 'enum_id': 79919,
                 'enum_code': 'WORK'}]}]}}
        return cnts

    costomers_ids = (', ').join([str(i) for i in deals_info['consumer_profile_id']])
    get_cnts = f"""SELECT company_id, contact_person, phone,email, name FROM `companies`
    where company_id in ({costomers_ids})"""

    cnts = query_df(get_cnts,token['wf_base'])

    contacts = []
    for cnt in cnts.itertuples():
        contacts.append(create_amo_cnt(cnt))
    leads_data  = [i['data'] for i in leads]

    # Загрузка данных по сделкам, контактам и их связка

    new_lead_ids = amo_connect.post_data( 'leads', leads_data)

    for i, lead_response in enumerate(new_lead_ids['_embedded']['leads']):
        leads[i]['lead_link'] = lead_response['id']

    cnt_data  = [i['data'] for i in contacts]

    cnt_ids = amo_connect.post_data('contacts',cnt_data)

    for i, cnt_response in enumerate(cnt_ids['_embedded']['contacts']):
        contacts[i]['lead_link'] = cnt_response['id']


    ctn_ids = {i['cnt_id']:i['lead_link'] for i in  contacts}

    for lead in leads:
        linked_entity = f"leads/{lead['lead_link']}/link"
        linking_data = [
            {
                "to_entity_id": ctn_ids[lead['cnt_id']],
                "to_entity_type": "contacts",
                "metadata": {
                    "is_main": True,
                }
            }
        ]
        linkage = amo_connect.post_data(linked_entity,linking_data)

    json_tracker['i-cap']['last_order'] = max([i['lead_id'] for i in leads])
    leads_and_states  = {str(i['lead_id']):{'amo_id':i['lead_link'],
                        'status':i['data']['status_id']} for i in leads}

    for i in leads_and_states:
        json_tracker['i-cap']['tracked_deals'][i] = leads_and_states[i]

    base_json.write(json_tracker)

# Функция обновления статуса сделок: 
# 1) Пиши ид сделок и статус в файл
# 2) Выгружай данные о сделках их файла и сравнивай с базой
# 3) Если есть разница - выгружай изменение
q_tarcked_deals = f"""SELECT deal_id, status FROM `deals`
where `supplier_company_id` = {supplier_company_id}
and deal_id >={json_tracker['i-cap']['first_tracked_deal']}
and deal_id <={deal_id}"""

tracked_deals_status_bd = query_df(q_tarcked_deals,token['wf_base'])
base_deal_states = {str(i.deal_id):{'bd_id':str(i.status)} for i in tracked_deals_status_bd.itertuples()}
funnels = json_tracker['i-cap']['funnels']
tracked_deals = json_tracker['i-cap']['tracked_deals']
for i in base_deal_states:
    base_deal_states[i]['status'] = funnels[base_deal_states[i]['bd_id']]
# Проверяем статусы
changes = {}
for i in base_deal_states:
    if base_deal_states[i]['status'] == tracked_deals[i]['status']:
        continue
    else:
        changes[i] = [tracked_deals[i]['amo_id'], base_deal_states[i]['status']]

def update_states(changes):
    for i in changes:
        patch_lead_link = f"leads/{str(changes[i][0])}"

        change = {
        'status_id' : changes[i][1]
        }
        amo_connect.patch(patch_lead_link,change)
        json_tracker['i-cap']['tracked_deals'][i]['status']  = changes[i][1]

if len(changes)> 0:
    update_states(changes)
    base_json.write(json_tracker)


print(f'время всего{str(datetime.datetime.today() - x')