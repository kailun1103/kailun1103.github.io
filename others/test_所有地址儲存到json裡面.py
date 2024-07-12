import os
import json

json_file_path = 'test'
output_filename = '2024_01_18-2024_01_24 - address.json'
unique_addresses = set()
total = 0
for root, dirs, files in os.walk(json_file_path):
    json_files = [file for file in files if file.endswith('.json')]
    for json_file in json_files:
        json_path = os.path.join(root, json_file)
        with open(json_path, 'r', encoding='utf-8') as infile:
            print(json_file)
            json_data = json.load(infile)
            for transaction in json_data:
                txn_input_details = json.loads(transaction['Txn Input Details'])
                txn_output_details = json.loads(transaction['Txn Output Details'])
                
                # 提取输入地址
                for input_detail in txn_input_details:
                    unique_addresses.add(input_detail['inputHash'])
                
                # 提取输出地址
                for output_detail in txn_output_details:
                    unique_addresses.add(output_detail['outputHash'])

with open(output_filename, 'w') as file:
    json.dump(list(unique_addresses), file, indent=4)
