import requests
import json

headers = {
    'Ok-Access-Key': 'b5bae772-4c29-49bd-912b-7005e275837a'
}
payload = {
    "chainShortName": "btc",
    "txid": "07d69a1e8a19b6632dc79c2cfbf064d7f7b204ae80ee089a9bd505028c6a6161"
}

response = requests.get("https://www.oklink.com/api/v5/explorer/transaction/transaction-fills", headers=headers, params=payload)
response_data = response.json()  # 将响应数据解析为 JSON 格式
print(response_data)
# print(response_data['data'][0]['txfee'])
    
