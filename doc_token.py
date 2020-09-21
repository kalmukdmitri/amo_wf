def get_tokens():
    import json
    from googleapiclient.discovery import build
    from oauth2client.service_account import ServiceAccountCredentials

    SCOPES = ['https://www.googleapis.com/auth/drive',
        'https://www.googleapis.com/auth/documents']
    
    credentials = ServiceAccountCredentials.from_json_keyfile_name('kalmuktech-5b35a5c2c8ec.json', SCOPES)
    docname = "1U_NdroZychiHGgHeslmlFvVUrdpFTwp5KIrMVnVwjWQ"
    service = build('docs', 'v1', credentials=credentials)
    googe_request = service.documents().get(documentId = docname).execute()
    token_str=googe_request['body']['content'][1]['paragraph']['elements'][0]['textRun']['content']
    token = json.loads(token_str.strip().replace("'", '"'))
    return token