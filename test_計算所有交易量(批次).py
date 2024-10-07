import os
import json
import time
from concurrent.futures import ProcessPoolExecutor

def count_dust_bool(json_path):
    with open(json_path, 'r', encoding='utf-8') as infile:
        print(infile)
        json_data = json.load(infile)
        return sum(1 for item in json_data if float(item['Txn Output Amount']) <= 0.00000546)
        # return sum(1 for item in json_data if item['Dust Bool'] == '1')

def main():
    json_file_path = '0619-0811/0619-0723'
    # json_file_path = 'dataset'
    start_time = time.time()

    json_files = []
    for root, _, files in os.walk(json_file_path):
        json_files.extend(os.path.join(root, file) for file in files if file.endswith('.json'))

    total = 0
    with ProcessPoolExecutor() as executor:
        results = executor.map(count_dust_bool, json_files)
        total = sum(results)

    end_time = time.time()
    execution_time = end_time - start_time

    # print("float(item['Miner Verification Time']) > 3600 and item['Dust Bool'] == '0'")
    print(f"總計: {total}")
    print(f"執行時間: {execution_time:.2f} 秒")

if __name__ == "__main__":
    main()