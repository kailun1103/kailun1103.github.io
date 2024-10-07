import requests
import json

headers = {
    'Ok-Access-Key': 'b5bae772-4c29-49bd-912b-7005e275837a'
}
payload = {
    "chainShortName": "btc",
    "txid": "86d953aea7460c4e40582dc64a5852669deb5797b5552651c86752dc821571fe"
}

response = requests.get("https://www.oklink.com/api/v5/explorer/transaction/transaction-fills", headers=headers, params=payload)
response_data = response.json()  # 将响应数据解析为 JSON 格式
print(response_data)
    
