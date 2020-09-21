import time
import requests
import json
import datetime
import pandas
import os
from google.cloud import bigquery
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import mysql.connector as mysql
from doc_token import get_tokens
import pandas as pd

SCOPES = ['https://www.googleapis.com/auth/drive',
        'https://www.googleapis.com/auth/documents']

credentials = ServiceAccountCredentials.from_json_keyfile_name('kalmuktech-5b35a5c2c8ec.json', SCOPES)
service = build('docs', 'v1', credentials=credentials)



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
    
    
amo_connect = get_AMO(current_token['access_token'])
        
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

from doc_token import get_tokens
import pandas as pd

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

token = get_tokens()
get_last_order = """SELECT * FROM `deals`
where showcase_id =73
order by dt_create desc
limit 1"""
deals_info = query_df(get_last_order,token['wf_base'])

get_order_products = f"""SELECT caption, cnt, price FROM `deal_good_offers`
where deal_id = {deals_info['deal_id'][0]}"""

deals_products = query_df(get_order_products,token['wf_base'])

get_cnts = f"""SELECT contact_person, phone,email, name FROM `companies`
where company_id = {deals_info['consumer_profile_id'][0]}"""

cnts = query_df(get_cnts,token['wf_base'])

def create_amo_lead(deals_info,deals_products):
    products = f"Заказали {str(datetime.datetime.utcfromtimestamp(deals_info['dt_create'][0]))} \n"
    for i in deals_products.iterrows():
        products += f"{i[1]['caption']}, {i[1]['cnt']} шт, {i[1]['price']} р за шт. \n"
    
    lead = {'name': f"Сделка из Workface #{deals_info['deal_id'][0]}",
     'account_id': 23428672,
     'pipeline_id': 3553822,
     'status_id': 34966699,
     'custom_fields_values': [{'field_id': 671251,
   'field_name': 'Дополнение',
   'field_code': None,
   'field_type': 'textarea',
   'values': [{'value': products}]}],
     'sale': int(deals_info['total_price'][0])}

    return lead
test_lead = create_amo_lead(deals_info,deals_products)
new_lead_id = amo_connect.post_leady_data(test_lead)

def create_amo_cnt(cnt):
    
    cnts = [{'name': str(cnt['contact_person'][0]),
           'custom_fields_values': [{'field_id': 50597,
           'field_name': 'Телефон',
           'field_code': 'PHONE',
           'field_type': 'multitext',
           'values': [{'value': str(cnt['phone'][0]),
             'enum_id': 79907,
             'enum_code': 'WORK'}]},
          {'field_id': 50599,
           'field_name': 'Email',
           'field_code': 'EMAIL',
           'field_type': 'multitext',
           'values': [{'value': str(cnt['email'][0]), 
             'enum_id': 79919,
             'enum_code': 'WORK'}]}]}]
    return cnts


cnttt = create_amo_cnt(cnts)
cnt_id = amo_connect.post_data('contacts',cnttt)

linked_entity = f"leads/{new_lead_id['_embedded']['leads'][0]['id']}/link"
linking_data = [
    {
        "to_entity_id": cnt_id['_embedded']['contacts'][0]['id'],
        "to_entity_type": "contacts",
        "metadata": {
            "is_main": True,
        }
    }
]

linkage = amo_connect.post_data(linked_entity,linking_data)

patch_lead_link = f"leads/{new_lead_id['_embedded']['leads'][0]['id']}"

change = {
    'status_id' : 34966702
}
amo_connect.patch(patch_lead_link,change )


