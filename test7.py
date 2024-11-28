import json
import numpy as np
from typing import List, Dict, Any

def calculate_quartiles(data: List[Dict[Any, Any]]) -> tuple:
    """計算Virtual Txn Size的四分位數"""
    sizes = [float(item['Virtual Txn Size']) for item in data]
    q1 = np.percentile(sizes, 25)
    q2 = np.percentile(sizes, 50)
    q3 = np.percentile(sizes, 75)
    return q1, q2, q3

def get_quartile_data(data: List[Dict[Any, Any]], quartiles: tuple) -> tuple:
    """將數據分到各個四分位區間"""
    q1, q2, q3 = quartiles
    q1_data = []
    q2_data = []
    q3_data = []
    q4_data = []
    
    for item in data:
        size = float(item['Virtual Txn Size'])
        if size <= q1:
            q1_data.append(item)
        elif size <= q2:
            q2_data.append(item)
        elif size <= q3:
            q3_data.append(item)
        else:
            q4_data.append(item)
            
    return q1_data, q2_data, q3_data, q4_data

def sample_json_data(input_file: str, output_file: str, sample_sizes: Dict[str, float]):
    """
    從每個四分位數中採樣指定比例的數據
    
    Args:
        input_file: 輸入檔案路徑
        output_file: 輸出檔案路徑
        sample_sizes: 每個四分位要採樣的比例，例如 {'q1': 0.3, 'q2': 0.25, 'q3': 0.25, 'q4': 0.2}
    """
    # 讀取JSON檔案
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # 根據Virtual Txn Size排序
    sorted_data = sorted(data, key=lambda x: float(x['Virtual Txn Size']))
    
    # 計算四分位數
    quartiles = calculate_quartiles(sorted_data)
    q1_data, q2_data, q3_data, q4_data = get_quartile_data(sorted_data, quartiles)
    
    # 從每個四分位數中採樣
    sampled_q1 = q1_data[:int(len(q1_data) * sample_sizes['q1'])]
    sampled_q2 = q2_data[:int(len(q2_data) * sample_sizes['q2'])]
    sampled_q3 = q3_data[:int(len(q3_data) * sample_sizes['q3'])]
    sampled_q4 = q4_data[:int(len(q4_data) * sample_sizes['q4'])]
    
    # 合併採樣結果
    sampled_data = sampled_q1 + sampled_q2 + sampled_q3 + sampled_q4
    
    # 寫入新的JSON檔案
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(sampled_data, f, ensure_ascii=False, indent=2)
    
    # 輸出統計資訊
    print(f"\n=== 原始資料統計 ===")
    print(f"Q1 資料數量: {len(q1_data)}")
    print(f"Q2 資料數量: {len(q2_data)}")
    print(f"Q3 資料數量: {len(q3_data)}")
    print(f"Q4 資料數量: {len(q4_data)}")
    print(f"總資料數量: {len(data)}")
    
    print(f"\n=== 採樣後資料統計 ===")
    print(f"Q1 採樣數量: {len(sampled_q1)} (保留 {sample_sizes['q1']*100}%)")
    print(f"Q2 採樣數量: {len(sampled_q2)} (保留 {sample_sizes['q2']*100}%)")
    print(f"Q3 採樣數量: {len(sampled_q3)} (保留 {sample_sizes['q3']*100}%)")
    print(f"Q4 採樣數量: {len(sampled_q4)} (保留 {sample_sizes['q4']*100}%)")
    print(f"總採樣數量: {len(sampled_data)}")
    
    print(f"\n=== Virtual Txn Size 範圍 ===")
    print(f"Q1: {min([float(x['Virtual Txn Size']) for x in q1_data])} - {max([float(x['Virtual Txn Size']) for x in q1_data])}")
    print(f"Q2: {min([float(x['Virtual Txn Size']) for x in q2_data])} - {max([float(x['Virtual Txn Size']) for x in q2_data])}")
    print(f"Q3: {min([float(x['Virtual Txn Size']) for x in q3_data])} - {max([float(x['Virtual Txn Size']) for x in q3_data])}")
    print(f"Q4: {min([float(x['Virtual Txn Size']) for x in q4_data])} - {max([float(x['Virtual Txn Size']) for x in q4_data])}")

def main():
    input_file = 'txn_merged_result_sampled.json'
    output_file = 'txn_merged_result_sampled_sampled.json'
    
    # 設定每個四分位要採樣的比例
    sample_sizes = {
        'q1': 0.64,  # 從Q1採樣30%
        'q2': 0.64, # 從Q2採樣25%
        'q3': 0.064, # 從Q3採樣25%
        'q4': 0.064   # 從Q4採樣20%
    }
    
    sample_json_data(input_file, output_file, sample_sizes)

if __name__ == "__main__":
    main()