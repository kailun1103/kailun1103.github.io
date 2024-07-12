import os
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

json_file_path = '2024_01_18-2024_01_24 - 資料集(json)'
output_filename = '2024_01_18-2024_01_24 - address2.json'
unique_addresses = []
total_files = 0
processed_files = 0

lock = Lock()

def process_file(json_path):
    local_unique_addresses = set()
    with open(json_path, 'r', encoding='utf-8') as infile:
        json_data = json.load(infile)
        for transaction in json_data:
            txn_input_details = json.loads(transaction['Txn Input Details'])
            txn_output_details = json.loads(transaction['Txn Output Details'])
            
            # 提取输入地址
            for input_detail in txn_input_details:
                local_unique_addresses.add(input_detail['inputHash'])
            
            # 提取输出地址
            for output_detail in txn_output_details:
                local_unique_addresses.add(output_detail['outputHash'])
    return local_unique_addresses

json_files = []
for root, dirs, files in os.walk(json_file_path):
    json_files.extend([os.path.join(root, file) for file in files if file.endswith('.json')])
total_files = len(json_files)

with ThreadPoolExecutor() as executor:
    futures = [executor.submit(process_file, json_file) for json_file in json_files]
    for future in as_completed(futures):
        unique_addresses.extend(future.result())
        with lock:
            processed_files += 1
            print(f"Processed {processed_files}/{total_files} files")

# 去重
unique_addresses = list(set(unique_addresses))

with open(output_filename, 'w') as file:
    json.dump(unique_addresses, file, indent=4)

print(f"Total unique addresses: {len(unique_addresses)}")
