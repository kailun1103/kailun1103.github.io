import json
import sys
import os
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor
import time

# Define multiple date-time formats
datetime_formats = [
    '%Y/%m/%d %H:%M',
    '%Y/%m/%d %I:%M:%S %p',
    '%Y-%m-%d %H:%M:%S',
    '%Y/%m/%d %H:%M:%S',
]

def parse_datetime(datetime_str):
    for fmt in datetime_formats:
        try:
            return datetime.strptime(datetime_str, fmt)
        except ValueError:
            continue
    raise ValueError(f"Time data '{datetime_str}' does not match any known formats.")

def process_file(args):
    json_path, txn_fields = args
    hourly_values = {field: {hour: [] for hour in range(24)} for field in txn_fields}
    with open(json_path, 'r', encoding='utf-8') as infile:
        json_data = json.load(infile)
        # print(infile)
        for item in json_data:
            if item['Dust Bool'] == '1' and item['Txn Input Address'] == '1' and item['Txn Output Address'] == '1':
                date = parse_datetime(item["Txn Initiation Date"])
                # date = parse_datetime(item["Txn Verification Date"])
                hour = date.hour
                for field in txn_fields:
                    try:
                        value = float(item[field])
                        hourly_values[field][hour].append(value)
                    except (ValueError, KeyError):
                        print(infile)
                        # print('error')
                        pass
    return hourly_values

def calculate_averages(hourly_values):
    averages = {}
    for field, hourly_value in hourly_values.items():
        averages[field] = {}
        for hour, value in hourly_value.items():
            if value:
                averages[field][hour] = sum(value) / len(value)
    return averages

def main():
    if len(sys.argv) < 3:
        print("Usage: python script.py <txn_field1> <txn_field2>")
        sys.exit(1)

    txn_fields = sys.argv[1:]

    json_file_path = '0619-0811/0619-0723'
    start_time = time.time()

    # Get all JSON files
    json_files = []
    for root, _, files in os.walk(json_file_path):
        json_files.extend(os.path.join(root, file) for file in files if file.endswith('.json'))

    # Process files in parallel
    hourly_values = {field: {hour: [] for hour in range(24)} for field in txn_fields}
    with ProcessPoolExecutor() as executor:
        results = executor.map(process_file, [(file, txn_fields) for file in json_files])
        for file_values in results:
            for field in txn_fields:
                for hour, value in file_values[field].items():
                    hourly_values[field][hour].extend(value)

    # Calculate averages
    hourly_averages = calculate_averages(hourly_values)

    # Print out the hourly averages for each field
    for field in txn_fields:
        print('')
        print(field)
        print('')
        for hour in range(24):
            if hour in hourly_averages[field]:
                print(f"{hourly_averages[field][hour]:.8f}")
            else:
                print(f"Hour {hour}: No data")

    end_time = time.time()
    execution_time = end_time - start_time
    print('')
    print(f"\nExecution time: {execution_time:.2f} seconds")

if __name__ == "__main__":
    main()

# python test_計算欄位每小時數量.py 'Txn Input Amount' 'Txn Output Amount' 'Txn Input Address' 'Txn Output Address' 'Txn Fee' 'Txn Weight' 'Txn Fee Rate' 'Txn Fee Ratio' 'Mempool Txn Count' 'Memory Depth' 'Miner Verification Time' 'Total Txn Size' 'Virtual Txn Size' 'Block Txn Count' 'Block Txn Amount' 'Block Size' 'Block Miner Reward' 'Block Txn Fees' 'Block Difficulty' 'Block Confirm'