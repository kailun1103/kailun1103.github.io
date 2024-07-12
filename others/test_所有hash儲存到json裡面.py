import os
import json

json_file_path = 'test'
output_filename = 'hash數量.json'
hashes_list = []
total = 0
for root, dirs, files in os.walk(json_file_path):
    json_files = [file for file in files if file.endswith('.json')]
    for json_file in json_files:
        json_path = os.path.join(root, json_file)
        with open(json_path, 'r', encoding='utf-8') as infile:
            print(json_file)
            json_data = json.load(infile)
            for transaction in json_data:
                hashes_list.append(transaction['Txn Hash'])


with open(output_filename, 'w') as file:
    json.dump(list(hashes_list), file, indent=4)
