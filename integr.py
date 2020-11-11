from gsql import gsql
from pandas import DataFrame as pd
from amo_class import get_AMO
import pymysql.cursors
import time
import requests
import json
import string
import datetime

with open('db_access.json') as json_file:
    keys = json.load(json_file)
    
gsql_token = keys['gsql_token']
wf_pass = keys['wf_pass']

g_db = gsql(gsql_token)

get_current_cmp = f"select * from domain_data"

def get_new_token_dp(cmp_id, g_db):
    
    old_token = g_db.get(f'select * from tokens where wf_cmp_id = {cmp_id}')
    d_token = {i:e[0] for i,e in dict(old_token).items()}


    url = d_token['amo_domain']+'oauth2/access_token'
    data = json.dumps({
    "client_id": d_token['client_id'],
    "client_secret": d_token['client_secret'],
    "grant_type": "refresh_token",
    'redirect_uri':d_token['redirect_uri'],
    "refresh_token": d_token['refresh_token']
                    })

    token_new = json.loads(requests.post(url, headers = {"Content-Type":"application/json"},data=data).text)
    d_token['token'] = token_new['access_token']

    if d_token['token'] != token_new['access_token']:
        changes_pd = pd([['token', "'"+d_token['token']+"'", 'wf_cmp_id', cmp_id]] , columns = ['changed_cols', 'changed_values' , 'case_cols' , 'case_vals'])
        update_query = g_db.update_df_q(changes_pd, 'tokens')[0]
        g_db.put(update_query)
    if d_token['refresh_token'] != token_new['refresh_token']:

        changes_pd = pd([['refresh_token', "'"+token_new['refresh_token']+"'", 'wf_cmp_id', cmp_id]] , columns = ['changed_cols', 'changed_values' , 'case_cols' , 'case_vals'])
        update_query = g_db.update_df_q(changes_pd, 'tokens')[0]
        g_db.put(update_query)
    
    return d_token


get_current_cmp = f"select * from domain_data"
current_clients=g_db.get(get_current_cmp)

integrated_client = []
for i in current_clients.iterrows():
    integrated_client.append(dict(i[1]))

for client in integrated_client[:1]:
    g_db.reopen() 
    client_json = client.copy()
    supplier_company_id = client_json['supplier_company_id']
    token = get_new_token_dp(supplier_company_id, g_db)
    amo_connect = get_AMO(token['token'], token['amo_domain'])
    dicts_amo = amo_connect.get_data("account?with=pipelines,custom_fields,users")
    pips = amo_connect.pipiline_loc(dicts_amo['_embedded']['pipelines'])
    
    if 'WorkFace' not in [pips[i][0] for i in pips]:
        creat_states_map = amo_connect.creat_new_funnels(supplier_company_id)
        df_funnles = pd(creat_states_map, columns=['wf_status','amo_fld_id','company_id'])
        qu_add_new_funnels = g_db.insert_pd(df_funnles, 'funnels')
        g_db.put(qu_add_new_funnels)
        
        fields_pd = amo_connect.create_custom_fields(supplier_company_id)
        qu_add_custom_fields = g_db.insert_pd(fields_pd, 'custom_fields')
        g_db.put(qu_add_custom_fields)
        
    amo_account_id = dicts_amo['id']
    
    deal_id = client_json['last_order']
    funnels_pd  = g_db.get(f'select wf_status, amo_fld_id from funnels where company_id = {supplier_company_id}')
    funnel_dict = {i.wf_status:i.amo_fld_id for i in  funnels_pd.itertuples()}
    
    wf_db = gsql(wf_pass)
    get_last_order = f"""SELECT * FROM `deals`
    where `supplier_company_id` = {supplier_company_id}
    and deal_id >{deal_id}"""
    

    customs = g_db.get(f"select * from custom_fields where wf_company_id = {supplier_company_id} ")
    customs_fields = {}
    for i in customs.itertuples():
        if i.entity_type in customs_fields:
            customs_fields[i.entity_type][i.name] = i.id
        else:
            customs_fields[i.entity_type] = {}
            customs_fields[i.entity_type][i.name] = i.id
    
    
    deals_info = wf_db.get(get_last_order)
    
    if len(deals_info) > 0:
        deals_ids = (', ').join([str(i) for i in deals_info['deal_id']])
        get_order_products = f"""SELECT deal_id, caption, cnt, price FROM `deal_good_offers`
        where deal_id in ( {deals_ids} )"""
        
        deals_products = wf_db.get(get_order_products)
        
        deals_data = {}

        for i in deals_products.itertuples():
            if i.deal_id in deals_data:
                deals_data[i.deal_id].append(i)
            else:
                deals_data[i.deal_id] = [i]
                
        def create_amo_lead(deals_info, deals_products, json_tracker, funnel_dict, amo_account_id, customs_fields):
            customs_fields = customs_fields['leads']
            products = f"Дата заказа {str(datetime.datetime.utcfromtimestamp(deals_info.dt_create))} \n"
            for i in deals_products[deals_info.deal_id]:
                products += f"{i.caption}, {i.cnt} шт, {i.price} р за шт. \n"

            lead = {
                'lead_id' : deals_info.deal_id,
                'cnt_id' : deals_info.consumer_profile_id,
                'data':{
                    'name': f"Сделка из Workface #{deals_info.deal_id}",
                    'account_id': amo_account_id,
                    'pipeline_id': json_tracker['pipeline_id'],
                    'status_id': funnel_dict[deals_info.status],
                    'custom_fields_values': [
                         {'field_id': customs_fields['Спецификация'],
                          'field_name': 'Спецификация',
                          'field_code': None,
                          'field_type': 'textarea',
                          'values': [{'value': products}]},
                         {'field_id': customs_fields['Тип оплаты'],
                          'field_name': 'Тип оплаты',
                          'field_code': None,
                          'field_type': 'text',
                          'values': [{'value': deals_info.payment_string}]},
                         {'field_id': customs_fields['Тип доставки'],
                          'field_name': 'Тип доставки',
                          'field_code': None,
                          'field_type': 'text',
                          'values': [{'value': deals_info.delivery_string}]},
                         {'field_id': customs_fields['Сделка на workface'],
                          'field_name': 'Сделка на workface',
                          'field_code': None,
                          'field_type': 'text',
                          'values': [{'value': f'https://workface.ru/ru/deal/{deals_info.deal_id}'}]},
                         {'field_id': customs_fields['Комментарий покупателя'],
                          'field_name': 'Комментарий покупателя',
                          'field_code': None,
                          'field_type': 'text',
                          'values': [{'value': deals_info.comment_consumer}]} 
                    ],
                    'price': int(deals_info.total_price)
                        }
                   }

            return lead
        leads = []
        for order in deals_info.itertuples():
            leads.append(create_amo_lead(order, deals_data,client_json, funnel_dict,amo_account_id, customs_fields))

        costomers_ids = (', ').join([str(i) for i in deals_info['consumer_profile_id']])
        get_cnts = f"""SELECT * FROM `companies`
        where company_id in ({costomers_ids})"""

        cnts = wf_db.get(get_cnts)
        def create_amo_cnt(cnt,customs_fields):
            customs_fields = customs_fields['contacts']
            cnts = {
                'phone': cnt.phone,
                'cnt_id' : cnt.company_id,
                'data':{'name': str(cnt.contact_person),
                   'custom_fields_values': [
                   {
                   'field_id': customs_fields['Телефон'],
                   'field_name': 'Телефон',
                   'field_code': 'PHONE',
                   'field_type': 'multitext',
                   'values': [{'value': str(cnt.phone),
                               'enum_code': 'WORK'}]
                   },
                  {
                   'field_id': customs_fields['Email'],
                   'field_name': 'Email',
                   'field_code': 'EMAIL',
                   'field_type': 'multitext',
                   'values': [{'value': str(cnt.email), 
                               'enum_code': 'WORK'}]
                  }
                   ]
                       }
            }
            return cnts

        costomers_ids = (', ').join([str(i) for i in deals_info['consumer_profile_id']])
        get_cnts = f"""SELECT * FROM `companies`
        where company_id in ({costomers_ids})"""

        cnts = wf_db.get(get_cnts)

        contacts = []
        for cnt in cnts.itertuples():
            contacts.append(create_amo_cnt(cnt,customs_fields))
            
        all_cnt = amo_connect.get_big_amo('contacts')
        
        def get_custom_phone(cstms , fld = 78683):
            for i in cstms:
                
                if 'id' in i and i['id'] == fld:
                    phn  = ''
                    for j in i['values'][0]['value']:

                        if j in string.digits:
                            phn += j

                    return phn

        cnt_map = [[i['id'],get_custom_phone(i['custom_fields'])] for i in all_cnt]
        for i in cnt_map:
            for j in contacts:
                if i[1] == j['phone']:
                    j['amo_cnt_id'] = i[0]
            leads_data  = [i['data'] for i in leads]
            
        new_lead_ids = amo_connect.post_data( 'leads', leads_data)

        for i, lead_response in enumerate(new_lead_ids['_embedded']['leads']):
            leads[i]['lead_link'] = lead_response['id']

        cnt_data  = [i['data'] for i in contacts if 'amo_cnt_id' not in i]

        if len(cnt_data)>0:
            cnt_ids = amo_connect.post_data('contacts',cnt_data)
            for i, cnt_response in enumerate(cnt_ids['_embedded']['contacts']):
                contacts[i]['amo_cnt_id'] = cnt_response['id']

        ctn_ids = {i['cnt_id']:i['amo_cnt_id'] for i in  contacts}

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
            
        def create_amo_cmp(cmp_data,customs_fields):
            customs_fields = customs_fields['companies']
            try:
                sub_companies = json.loads(cmp_data.sub_companies)
                for i in sub_companies:
                    if i['main']:
                        data = ""
                        requisites_lst = {"sub-company-inn": "ИНН",
                                          "sub-company-legal-address": "Юр. Адресс",
                                         }

                        for j in requisites_lst:
                            data += str(requisites_lst[j])+' '+i[j]+"\n"
                requisites = data[:-3]
            except:
                requisites = ""
            try:
                cmp_adress = json.loads(cmp_data.address)['text']
            except:
                cmp_adress = ""
            cnts = {
            'name': cmp_data.name,
            'cnt_id' : cmp_data.company_id,
            'data':{'name': str(cmp_data.name),
                   'custom_fields_values': [
                    {
                     'field_id': customs_fields['Адрес'],
                     'field_name': 'Адрес',
                     'values': [{'value': str(cmp_adress)}]
                    },
                    {
                     'field_id': customs_fields['Реквизиты'],
                     'field_name': 'Реквизиты',
                     'values': [{'value': str(requisites)}]
                    }
                   ]
                }
            }    
            return cnts

        companies = []
        for cnt in cnts.itertuples():
            companies.append(create_amo_cmp(cnt,customs_fields))

        all_cmps = amo_connect.get_big_amo('companies')
        cmp_maps = [[i['id'],i['name']] for i in all_cmps]
        for i in cmp_maps:
            for j in companies:
                if i[1] == j['name']:
                    j['amo_cmp_id'] = i[0]

        cmp_data  = [i['data'] for i in companies if 'amo_cmp_id' not in i]

        if len(cmp_data)>0:
            cnt_ids = amo_connect.post_data('companies',cmp_data)
            for i, cnt_response in enumerate(cnt_ids['_embedded']['contacts']):
                contacts[i]['amo_cnt_id'] = cnt_response['id']

        cmp_ids = {i['cnt_id']:i['amo_cmp_id'] for i in companies}


        for lead in leads:
            linked_entity = f"leads/{lead['lead_link']}/link"
            linking_data = [
                {
                    "to_entity_id": cmp_ids[lead['cnt_id']],
                    "to_entity_type": "companies",
                }
            ]
            linkage = amo_connect.post_data(linked_entity,linking_data)
        
        leads_states = [[i['lead_id'],i['lead_link'],i['data']['status_id'],supplier_company_id ] for i in leads]
        pd_new_leads = pd(leads_states, columns = ['wf_id', 'amo_deal_id', 'amo_status', 'supplier_company_id'])
        q_new_leads = g_db.insert_pd(pd_new_leads , 'tracked_deals')
        g_db.put(q_new_leads)
        
        max_lead = [max([i['lead_id'] for i in leads])]
        updatable = pd([max_lead], columns = ['last_order'])
        client['last_order'] = max_lead[0]
        cases = ['supplier_company_id', client['supplier_company_id']]
        q_update_base_leads = g_db.update_pd(updatable, 'domain_data' ,cases)
        for updates_q in q_update_base_leads:
            g_db.put(updates_q)
    

    wf_db.reopen()
    
    q_tarcked_deals = f"""SELECT deal_id, status FROM `deals`
    where `supplier_company_id` = {client['supplier_company_id']}
    and deal_id >={client['first_tracked_deal']}
    and deal_id <={client['last_order']}"""
    tracked_deals_status_bd = wf_db.get(q_tarcked_deals)
    wf_base_deal_states = {i.deal_id:funnel_dict[i.status] for i in tracked_deals_status_bd.itertuples()}

    g_base_deals  = g_db.get(f"""select  wf_id, amo_deal_id,amo_status from tracked_deals where supplier_company_id = {client['supplier_company_id']}""")
    g_base_dict = {i.wf_id:{'amo_deal_id':i.amo_deal_id, 'amo_status':i.amo_status} for i in g_base_deals.itertuples()}

    changes = {}

    for i in g_base_dict:
        if g_base_dict[i]['amo_status'] != wf_base_deal_states[i]:
            changes[g_base_dict[i]['amo_deal_id']] = wf_base_deal_states[i]

    if len(changes) > 0:

        def update_df_q(df_changes, table):
            queries = []
            base  = f"UPDATE {table} SET "
            for i in df_changes.itertuples():
                vals = f"{i.changed_cols} = {i.changed_values}"
                case = f"  WHERE {i.case_cols} = {i.case_vals}"
                query = base+vals+case
                queries.append(query)
            return queries

        changes_list = []

        for i,e in changes.items():

            change = ['amo_status', e, 'amo_deal_id', i]
            changes_list.append(change)

        changes_pd = pd(changes_list , columns = ['changed_cols', 'changed_values' , 'case_cols' , 'case_vals'])
        updates_list = update_df_q(changes_pd, 'tracked_deals')
        
        def update_states(changes):
            for i in changes:
                patch_lead_link = f"leads/{str(i)}"

                change = {
                'status_id' : changes[i]
                }
                amo_connect.patch(patch_lead_link,change)
        
        update_states(changes)
        
        for i in updates_list:
            g_db.put(i)