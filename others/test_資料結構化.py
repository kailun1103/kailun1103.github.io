import os
import json

json_file_path = '2024_01_18-2024_01_24 - 資料集(json)'

for root, dirs, files in os.walk(json_file_path):
    json_files = [file for file in files if file.endswith('.json')]
    for json_file in json_files:
        json_path = os.path.join(root, json_file)
        with open(json_path, 'r', encoding='utf-8') as infile:
            print(json_file)
            json_data = json.load(infile)
            for transaction in json_data:
                transaction['Txn Fees'] = f"{float(transaction['Txn Fees']):.8f}"
                transaction['Txn Input Amount'] = f"{float(transaction['Txn Input Amount']):.8f}"
                transaction['Txn Output Amount'] = f"{float(transaction['Txn Output Amount']):.8f}"

                txn_input_details = json.loads(transaction['Txn Input Details'])
                txn_output_details = json.loads(transaction['Txn Output Details'])

                # Check txn_input_details and set 'outputHash' to '123' if it is ''
                for input_detail in txn_input_details:
                    if 'inputHash' in input_detail and input_detail['inputHash'] == '':
                        input_detail['inputHash'] = 'Unknown_' + transaction['Txn Hash'][::10]

                # Check txn_output_details and set 'outputHash' to '123' if it is ''
                for output_detail in txn_output_details:
                    if 'outputHash' in output_detail and output_detail['outputHash'] == '':
                        output_detail['outputHash'] = 'Unknown_' + transaction['Txn Hash'][::10]

                # Print modified txn_output_details for verification

                # Update the transaction details in the original data
                transaction['Txn Input Details'] = json.dumps(txn_input_details)
                transaction['Txn Output Details'] = json.dumps(txn_output_details)

        # Save the modified data back to the file
        with open(json_path, 'w', encoding='utf-8') as outfile:
            json.dump(json_data, outfile, ensure_ascii=False, indent=4)
