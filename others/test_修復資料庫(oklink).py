import os
import json
import requests
import time

headers = {
    'Ok-Access-Key': 'b5bae772-4c29-49bd-912b-7005e275837a'
}

json_file_path = 'test'

total = 0
for root, dirs, files in os.walk(json_file_path):
    json_files = [file for file in files if file.endswith('.json')]
    for json_file in json_files:
        json_path = os.path.join(root, json_file)
        with open(json_path, 'r', encoding='utf-8') as infile:
            json_data = json.load(infile)
            count = 0
            print(json_file)
            for item in json_data:
                if int(item['Txn Input Address']) > 60 or int(item['Txn Output Address']) > 60:
                    payload = {
                        "chainShortName": "btc",
                        "txid": item['Txn Hash']
                    }
                    response = requests.get("https://www.oklink.com/api/v5/explorer/transaction/transaction-fills", headers=headers, params=payload)
                    time.sleep(0.2)
                    while response.status_code != 200:
                        time.sleep(5)
                        print(response.status_code)
                        response = requests.get("https://www.oklink.com/api/v5/explorer/transaction/transaction-fills", headers=headers, params=payload)
                    response_data = response.json()  # 将响应数据解析为 JSON 格式
                    if response_data['data'] != []:
                        input_details = json.dumps(response_data['data'][0]['inputDetails'], ensure_ascii=False)
                        output_details = json.dumps(response_data['data'][0]['outputDetails'], ensure_ascii=False)
                        item['Txn Input Details'] = input_details
                        item['Txn Output Details'] = output_details
                        count += 1
            total += count

        # 儲存修改後的資料
        with open(json_path, 'w', encoding='utf-8') as outfile:
            json.dump(json_data, outfile, ensure_ascii=False, indent=4)

print(total)