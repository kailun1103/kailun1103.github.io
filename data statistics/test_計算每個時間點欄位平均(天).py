import csv
import os
from datetime import datetime

# 定義多種日期時間格式
datetime_formats = [
    '%Y/%m/%d %H:%M',  # 例如：2024/1/18 00:00
    '%Y/%m/%d %I:%M:%S %p',  # 例如：2024/01/18 12:02:11 AM
    '%Y-%m-%d %H:%M:%S',  # 例如：2024-01-19 01:44:01
]

def parse_datetime(datetime_str):
    for fmt in datetime_formats:
        try:
            return datetime.strptime(datetime_str, fmt)
        except ValueError:
            continue
    raise ValueError(f"Time data '{datetime_str}' does not match any known formats.")

# 設定最大字段大小以避免字段錯誤
csv.field_size_limit(2147483647)
csv_file_path = '2024_01_18-2024_01_24 - 資料集'

# 字典用於保存每小時的計數和總和
hourly_data = {hour: {'count': 0, 'sum': 0.0} for hour in range(24)}

# 遍歷文件夾和文件
for root, dirs, files in os.walk(csv_file_path):
    csv_files = [file for file in files if file.endswith('.csv')]
    for csv_file in csv_files:
        print(csv_file)
        csv_path = os.path.join(root, csv_file)
        with open(csv_path, mode='r', newline='', encoding='utf-8') as infile:
            reader = csv.reader(infile)
            next(reader)  # 跳過標題
            for row in reader:
                if row[15] == '0':
                    date = parse_datetime(row[1])
                    hour = date.hour
                    value = float(row[5])
                    hourly_data[hour]['count'] += 1
                    hourly_data[hour]['sum'] += value

target_hours = [7,8,9,10,11,12,13,14,15,16,20,21]
total_count = 0
total_sum = 0

for hour in range(24):
    count = hourly_data[hour]['count']
    sum_value = hourly_data[hour]['sum']
    
    print(f'hour: {hour}')
    print(f'count: {count}')
    print(f'sum: {sum_value}')
    print('------------------------------')
    
    if hour in target_hours:
        total_count += count
        total_sum += sum_value

if total_count > 0:
    average = total_sum / total_count
else:
    average = 0

print('Txn Output Address')
print(f'Target hours: {target_hours}')
print(f'Total count: {total_count}')
print(f'Total sum: {total_sum}')
print(f'Average: {average:.8f}')