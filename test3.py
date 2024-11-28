import os
import json
from datetime import datetime

# def convert_to_timestamp(date_str):
#     try:
#         # 嘗試不同的日期格式
#         try:
#             dt = datetime.strptime(date_str, "%Y/%m/%d %H:%M:%S")
#         except ValueError:
#             try:
#                 dt = datetime.strptime(date_str, "%Y/%m/%d %H:%M")
#             except ValueError:
#                 dt = datetime.strptime(date_str, "%Y/%m/%d")
        
#         # 轉換為timestamp（毫秒）
#         timestamp = int(dt.timestamp() * 1000)
#         return timestamp
#     except ValueError as e:
#         print(f"日期轉換錯誤: {date_str}, 錯誤: {e}")
#         return None

def is_time_in_range(time_str):
    # 從日期時間字串中解析出時間
    try:
        # 首先嘗試帶秒的格式
        try:
            time = datetime.strptime(time_str, "%Y/%m/%d %H:%M:%S")
        except ValueError:
            # 如果失敗，嘗試不帶秒的格式
            time = datetime.strptime(time_str, "%Y/%m/%d %H:%M")
        
        return time.hour == 12
    except ValueError as e:
        print(f"無法解析時間格式: {time_str}, 錯誤: {e}")
        return False

def process_json_files(json_file_path):
    total_transactions = 0
    dust_transactions = 0
    
    for root, dirs, files in os.walk(json_file_path):
        json_files = [file for file in files if file.endswith('.json')]
        
        for json_file in json_files:
            json_path = os.path.join(root, json_file)
            print(f"處理文件: {json_file}")
            
            with open(json_path, 'r', encoding='utf-8') as infile:
                json_data = json.load(infile)
                
                for item in json_data:
                    if item['Dust Bool']=='1':
                        dust_transactions += 1
                        time_str = item.get('Txn Initiation Date')
                        if time_str and is_time_in_range(time_str):
                            block_str = item.get('Block Date')
                            if block_str and is_time_in_range(block_str):
                                total_transactions += 1

        print('總共')
        print(total_transactions)
        print(dust_transactions)
                        


# 使用示例
json_file_path = '0619-0811'
process_json_files(json_file_path)
