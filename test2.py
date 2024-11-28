import json

def load_address_data(path):
    """Load address data once and return as a set for efficient lookups"""
    with open(path, 'r', encoding='utf-8') as infile:
        add_data = json.load(infile)
        return {add['address'] for add in add_data}

def process_transaction_data(json_path, address_path):
    """Process transaction data and update cluster information"""
    # Load address data once
    dust_addresses = load_address_data(address_path)
    
    # Load transaction data
    with open(json_path, 'r', encoding='utf-8') as infile:
        json_data = json.load(infile)
    
    # Process each transaction
    for item in json_data:
        print(f"Processing transaction: {item['Txn Hash']}")
        
        # Process input details
        input_details = json.loads(item['Txn Input Details'])
        for detail in input_details:
            # Check if input hash is in dust addresses
            if detail['inputHash'] in dust_addresses:
                detail['cluster'] = '-1_dust'
        item['Txn Input Details'] = json.dumps(input_details)
        
        # Process output details
        output_details = json.loads(item['Txn Output Details'])
        for detail in output_details:
            # Check if output hash is in dust addresses
            if detail['outputHash'] in dust_addresses:
                detail['cluster'] = '-1_dust'
        item['Txn Output Details'] = json.dumps(output_details)
    
    # Write updated data back to file
    with open(json_path, 'w', encoding='utf-8') as outfile:
        json.dump(json_data, outfile, indent=2)

# File paths
add_path = '20241007_address_txn_statistics_cluster=-1,all-dust.json'
json_path = 'BTX_Transaction_data_2024_01_18_12-13 copy.json'

# Process the data
process_transaction_data(json_path, add_path)