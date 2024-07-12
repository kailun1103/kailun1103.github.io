import requests


headers = {
    'Ok-Access-Key': 'b5bae772-4c29-49bd-912b-7005e275837a'
}
payload = {
    "chainShortName": "btc",
    "txid": "06b39b31bbc91c578b22cb1c5f1ab587d2b6212273c8b85938cac4eb4f6d730e"
}

response = requests.get("https://www.oklink.com/api/v5/explorer/transaction/transaction-fills", headers=headers, params=payload)
response_data = response.json()  # 将响应数据解析为 JSON 格式
if response_data['data'] == []:
    print(response_data['data'])

