import os
import json
from multiprocessing import Pool, cpu_count
from functools import partial

def process_file(json_file, root):
    json_path = os.path.join(root, json_file)
    with open(json_path, 'r', encoding='utf-8') as infile:
        print(json_file)
        json_data = json.load(infile)
        count = 0
        output_amount = 0
        for item in json_data:
            # if float(item['Txn Fee Ratio']) >= 30:
            if item['Dust Bool'] == '1':
                count += 1
                output_amount += float(item['Virtual Txn Size'])
    return count, output_amount

def main():
    json_file_path = '0619-0811/0619-0723'
    all_json_files = []

    for root, dirs, files in os.walk(json_file_path):
        json_files = [file for file in files if file.endswith('.json')]
        all_json_files.extend([(file, root) for file in json_files])

    with Pool(processes=cpu_count()) as pool:
        results = pool.starmap(process_file, all_json_files)

    total_count = sum(result[0] for result in results)
    total_output_amount = sum(result[1] for result in results)

    print(f"Total count of transactions with Txn Fee Ratio: {total_count}")
    if total_count > 0:
        average_output_amount = total_output_amount / total_count
        print(f"Average Txn Output Amount for these transactions: {average_output_amount:.8f}")
    else:
        print("No transactions found with Txn Fee Ratio")

if __name__ == '__main__':
    main()