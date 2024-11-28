import os
import json
import time
from concurrent.futures import ProcessPoolExecutor

def count_dust_bool(json_path):
    try:
        with open(json_path, 'r', encoding='utf-8') as infile:
            json_data = json.load(infile)
            count = 0
            for item in json_data:
                # 檢查 'Txn Output Details' 是否存在
                if 'Txn Output Details' not in item:
                    print(f"Warning: 'Txn Output Details' not found in {json_path}")
                    print(f"Available keys: {item.keys()}")
                count += 1
            return count
    except Exception as e:
        print(f"Error processing {json_path}: {str(e)}")
        return 0

def main():
    json_file_path = 'gcn_dataset for address/all dust'
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

    print(f"總計: {total}")
    print(f"執行時間: {execution_time:.2f} 秒")

if __name__ == "__main__":
    main()