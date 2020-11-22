import requests
import json
from pandas import DataFrame as pd

class get_AMO:
    def __init__(self, token,domain):
        self.domain = domain 
        self.headers = {
        'Authorization': f"Bearer {token}",
        "Content-Type":"application/json"
        }
    def get_data(self, prm):
        url = f"{self.domain}api/v2/{prm}"
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
        url = f"{self.domain}api/v4/leads"
        print(url)

        data = json.dumps({'body':data})
        reqv = requests.post(url, headers = self.headers, data=data)
        print(reqv)
        print(json.loads(reqv.text))
        return json.loads(reqv.text)

    def post_data(self,prms, data):
        url = f"{self.domain}api/v4/{prms}"
        print(url)

        data = json.dumps( data)
        print(data)
        reqv = requests.post(url, headers = self.headers, data=data)
        print(reqv)
        return json.loads(reqv.text)

    def get(self, url):
        reqv = requests.get(url, headers = self.headers)
        return reqv

    def patch(self, prms, data):
        url = f"{self.domain}api/v4/{prms}"
        print(url)     

        data = json.dumps(data)
        print(data)
        reqv = requests.patch(url, headers = self.headers, data=data)       
        return json.loads(reqv.text)
    
    def pipiline_loc(self, pipelines):
        pipeline_dicts = {}
        for i in pipelines:

            pipeline_dicts[i] = [pipelines[i]['name']]
            statuse = {}

            for j in pipelines[i]['statuses']:
                statuse[j] = pipelines[i]['statuses'][j]['name']

            pipeline_dicts[i].append(statuse)
        return pipeline_dicts
    def creat_new_funnels(self,company_id):
        funnels =  [{'name': 'WorkFace',
         'sort': 10,
         'is_main': False,
         'is_unsorted_on' : False,
         '_embedded': {'statuses': [ 

                        {
                            "name": "Новая",
                            "sort": 20,

                        },
                        {
                            "name": "На ознакомлении",
                            "sort": 30,

                        },
                        {
                            "name": "Подтверждена",
                            "sort": 40,

                        },
                        {
                            "name": "На выполнении",
                            "sort": 50,

                        },
                        {
                            "name": "Отгрузка",
                            "sort": 60,
                        }, 
                        {
                            "id": 142,
                            "name": "Завершена"
                        },
                        {
                            "id": 143,
                            "name": "Отменена"
                        }

                    ]}
                    }
                   ]
        piplelines_create = self.post_data( 'leads/pipelines', funnels)
        map_states={'Новая':[5,2],
                    143:[4],
                    'На ознакомлении':[7,3],
                    'Подтверждена':[1,8],
                    'На выполнении':[9],
                    'Отгрузка':[10],
                    142:[11]}
        created_funnel_json = piplelines_create['_embedded']['pipelines'][0]['_embedded']['statuses']
        key_states = {i['name']:i['id'] for i in created_funnel_json}
        creat_states_map = []
        for state_name in map_states:
            if state_name in key_states:
                for state in map_states[state_name]:
                    creat_states_map.append([state, key_states[state_name], company_id])    
            else:
                for state in map_states[state_name]:
                    for s_id in map_states[state_name]:
                        creat_states_map.append([state, state_name,company_id])
        return creat_states_map
    
    def create_custom_fields(self, company_id):
    
        lead_data_to_post = [
                             {'name': 'Спецификация',
                              "sort": 510,
                              'type': 'textarea'
                             },
                             {
                              'name': 'Тип оплаты',
                              "sort": 511,
                              'type': 'text'
                             },
                             {
                              'name': 'Тип доставки',
                              "sort": 512,
                              'type': 'text'
                             },
                             {
                              'name': 'Сделка на workface',
                              "sort": 513,
                              'type': 'text'
                             },
                             {
                              'name': 'Комментарий покупателя',
                              "sort": 514,
                              'type': 'text'
                             }
                            ]


        custom_flds = self.post_data('leads/custom_fields',lead_data_to_post)['_embedded']['custom_fields']

        cnt_custom_to_post =  [
                           {
                           'name': 'Телефон',
                           'sort': 515,
                           'type': 'textarea'
                           },
                           {
                           'name': 'Email',
                           'sort': 516,
                           'type': 'textarea'
                           }
                          ]
        custom_flds += self.post_data('contacts/custom_fields',cnt_custom_to_post)['_embedded']['custom_fields']

        companies_custom_to_post = [
                                {
                                'name': 'Адрес',
                                'sort': 515,
                                'type': 'text'
                                },
                                {
                                'name': 'Реквизиты',
                                'sort': 515,
                                'type': 'text'
                                }
                               ]
        custom_flds += self.post_data('companies/custom_fields',companies_custom_to_post)['_embedded']['custom_fields']

        df_custom = pd(custom_flds)
        drop_columns = ['code','is_api_only','enums',
                        'request_id','required_statuses',
                        'is_deletable','remind','_links',
                        'group_id','is_predefined','sort']
        df_custom = df_custom.drop(drop_columns, axis= 1)

        df_custom['wf_company_id'] = company_id

        return df_custom
    
    def post_notes(self, lead_id, text):
        
        data = [{
                "note_type": "common",
                "params": {
                    "text": str(text)
                }}]

        url = f"{self.domain}api/v4/leads/{str(lead_id)}/notes"
        data = json.dumps(data)
        reqv = requests.post(url, headers = self.headers, data=data)
        return reqv
    
    def creat_new_funnels2(self,company_id):
        funnels =  [{'name': 'Воронка WorkFace',
         'sort': 10,
         'is_main': False,
         'is_unsorted_on' : False,
         '_embedded': {'statuses': [ 

                        {
                            "name": "ЛИД",
                            "sort": 20,

                        },
                        {
                            "name": "КОНТАКТ УСТАНОВЛЕН",
                            "sort": 30,

                        },
                        {
                            "name": "ПОТРЕБНОСТЬ ВЫЯВЛЕНА",
                            "sort": 40,

                        },
                        {
                            "name": "СЧЕТ-ДОГОВОР ВЫСТАВЛЕН/ОПЛАЧЕН",
                            "sort": 50,

                        },
                        {
                            "name": "ОТГРУЗКА/ДОСТАВКА",
                            "sort": 60,
                        },  
                        {
                            "id": 142,
                            "name": "Завершена"
                        },
                        {
                            "id": 143,
                            "name": "Отменена"
                        }

                    ]}
                    }
                   ]
        piplelines_create = self.post_data( 'leads/pipelines', funnels)
        map_states={'ЛИД':[1,2],
                    143:[4],
                    'КОНТАКТ УСТАНОВЛЕН':[7,3,5],
                    'ПОТРЕБНОСТЬ ВЫЯВЛЕНА':[8],
                    'СЧЕТ-ДОГОВОР ВЫСТАВЛЕН/ОПЛАЧЕН':[9],
                    'ОТГРУЗКА/ДОСТАВКА':[10],
                    142:[11]}
        created_funnel_json = piplelines_create['_embedded']['pipelines'][0]['_embedded']['statuses']
        print(created_funnel_json)
        key_states = {i['name']:i['id'] for i in created_funnel_json}
        print(key_states)
        creat_states_map = []
        for state_name in map_states:
            if state_name in key_states:
                for state in map_states[state_name]:
                    creat_states_map.append([state, key_states[state_name], company_id])    
            else:
                for state in map_states[state_name]:
                    for s_id in map_states[state_name]:
                        creat_states_map.append([state, state_name,company_id])
        return creat_states_map
