import os
import json
from multiprocessing import Pool, cpu_count
from functools import partial

def load_all_data(json_path):
    """載入所有資料到記憶體中"""
    with open(json_path, 'r', encoding='utf-8') as infile:
        return json.load(infile)

def process_single_item(item, all_data):
    print(item['Txn Hash'])
    """處理單一交易項目"""
    multi_layer = False
    
    # 檢查輸入地址
    input_addresses = json.loads(item['Txn Input Details'])
    for input_ in input_addresses:
        for item2 in all_data:
            output_addresses = json.loads(item2['Txn Output Details'])
            if any(input_['inputHash'] == output_['outputHash'] for output_ in output_addresses):
                multi_layer = True
                return (True, item)
    
    # 檢查輸出地址
    output_addresses = json.loads(item['Txn Output Details'])
    for output_ in output_addresses:
        for item2 in all_data:
            input_addresses = json.loads(item2['Txn Input Details'])
            if any(input_['inputHash'] == output_['outputHash'] for input_ in input_addresses):
                multi_layer = True
                return (True, item)
    
    return (False, item)

def process_chunk(items_chunk, all_data):
    """處理一個區塊的交易"""
    results = []
    for item in items_chunk:
        results.append(process_single_item(item, all_data))
    return results

def parallel_process_json(json_path, num_processes=None):
    """平行處理主函數"""
    # 如果沒有指定處理程序數量，使用 CPU 核心數量減 1
    if num_processes is None:
        num_processes = max(1, cpu_count() - 1)
    
    print(f"Using {num_processes} processes")
    
    # 載入所有資料
    all_data = load_all_data(json_path)
    total_items = len(all_data)
    
    # 將資料分割成大致相等的區塊
    chunk_size = total_items // num_processes
    if chunk_size == 0:
        chunk_size = 1
    chunks = [all_data[i:i + chunk_size] for i in range(0, total_items, chunk_size)]
    
    # 建立部分函數，固定 all_data 參數
    process_chunk_partial = partial(process_chunk, all_data=all_data)
    
    # 使用進程池進行平行處理
    with Pool(processes=num_processes) as pool:
        chunk_results = pool.map(process_chunk_partial, chunks)
    
    # 合併結果
    multi_items = []
    one_items = []
    count = 0
    
    for chunk_result in chunk_results:
        for is_multi, item in chunk_result:
            if is_multi:
                multi_items.append(item)
                count += 1
            else:
                one_items.append(item)
    
    # 寫入結果檔案
    with open('multi_layer(all dust).json', 'w', encoding='utf-8') as multi_file:
        json.dump(multi_items, multi_file, ensure_ascii=False, indent=2)
    
    with open('one_layer(all dust).json', 'w', encoding='utf-8') as one_file:
        json.dump(one_items, one_file, ensure_ascii=False, indent=2)
    
    return count, len(multi_items), len(one_items)

def main():
    json_path = 'BTX_Transaction_data_2024_01_18_12-13.json'
    count, multi_count, one_count = parallel_process_json(json_path)
    
    print(f"Total multi-layer transactions: {count}")
    print(f"Multi-layer items written: {multi_count}")
    print(f"One-layer items written: {one_count}")

if __name__ == "__main__":
    main()