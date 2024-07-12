import json
import time
import os
from py2neo import Graph, Node, Relationship, Subgraph


def create_subgraph(txn_hash, Txn_Input_Details, Txn_Output_Details, address_cache, transaction):
    nodes = []
    relationships = []

    # Create Transaction node
    txn_node = Node("Transaction", 
            TxnHash=txn_hash, 
            TxnInitiationDate=transaction['Txn Initiation Date'], 
            TxnInputAmount=transaction['Txn Input Amount'], 
            TxnOutputAmount=transaction['Txn Output Amount'], 
            TxnInputAddress=transaction['Txn Input Address'], 
            TxnOutputAddress=transaction['Txn Output Address'], 
            TxnFees=transaction['Txn Fees'], 
            MempoolCount=transaction['Mempool Count'], 
            MempoolFinalTxnDate=transaction['Mempool Final Txn Date'], 
            TxnState=transaction['Txn State'], 
            TxnVerificationDate=transaction['Txn Verification Date'], 
            BlockHeight=transaction['Block Height'], 
            MinerVerificationTime=transaction['Miner Verification Time'], 
            DustBool=transaction['Dust Bool'], 
            BlockHash=transaction['Block Hash'], 
            BlockValidator=transaction['Block Validator'], 
            BlockDate=transaction['Block Date'], 
            BlockTxnCount=transaction['Block Txn Count'], 
            BlockTxnAmount=transaction['Block Txn Amount'], 
            BlockSize=transaction['Block Size'], 
            MinerReward=transaction['Miner Reward'], 
            BlockTxnFees=transaction['Block Txn Fees'], 
            BlockMerkleRootHash=transaction['Block Merkle Root Hash'], 
            BlockMinerHash=transaction['Block Miner Hash'], 
            BlockDifficulty=transaction['Block Difficulty'], 
            BlockNonce=transaction['Block Nonce'], 
            BlockConfirm=transaction['Block Confirm']
    )

    nodes.append(txn_node)

    # Create input Address nodes and SENT relationships using cache
    for inp in Txn_Input_Details:
        input_address = inp['inputHash']
        if input_address not in address_cache:
            address_node = Node("Address", address=input_address)
            address_cache[input_address] = address_node
        else:
            address_node = address_cache[input_address]
        sent_rel = Relationship(address_node, "SENT", txn_node, amount=inp['amount'])
        relationships.append(sent_rel)

    # Create output Address nodes and RECEIVED relationships using cache
    for out in Txn_Output_Details:
        output_address = out['outputHash']
        if output_address not in address_cache:
            address_node = Node("Address", address=output_address)
            address_cache[output_address] = address_node
        else:
            address_node = address_cache[output_address]
        received_rel = Relationship(txn_node, "RECEIVED", address_node, amount=out['amount'])
        relationships.append(received_rel)

    return Subgraph(nodes, relationships)

def process_transactions(graph_uri, auth, database, json_data, batch_size, json_file, file_counter, file_total, address_cache):
    graph = Graph(graph_uri, auth=auth, name=database)
    combined_subgraph = Subgraph()
    
    
    for index, transaction in enumerate(json_data, start=1):
        txn_hash = transaction['Txn Hash']
        Txn_Input_Details = json.loads(transaction['Txn Input Details'])
        Txn_Output_Details = json.loads(transaction['Txn Output Details'])
        subgraph = create_subgraph(txn_hash, Txn_Input_Details, Txn_Output_Details, address_cache, transaction)
        combined_subgraph |= subgraph
        
        # Write to the database every batch_size transactions
        if index % batch_size == 0:
            graph.create(combined_subgraph)
            combined_subgraph = Subgraph()  # Reset subgraph
            print(f"Processing file {file_counter}/{file_total}: {json_file}, Processed batch {index//batch_size}, transaction {index}/{len(json_data)}, catch length: {len(address_cache)}")

    # Process remaining transactions
    if combined_subgraph:
        graph.create(combined_subgraph)

if __name__ == '__main__':
    URI = "bolt://localhost:7687"
    AUTH = ("kailun1103", "00000000")
    DATABASE = "btc"  # Specify your database name here
    address_cache = {}

    start_time = time.time()

    # json_file_path = '2024_01_18-2024_01_24 - 資料集(json)'
    json_file_path = '2024_01_18-2024_01_24 - 資料集(json)'
    file_total = sum(len(files) for _, _, files in os.walk(json_file_path) if any(file.endswith('.json') for file in files))
    file_counter = 1
    for root, dirs, files in os.walk(json_file_path):
        json_files = [file for file in files if file.endswith('.json')]
        for json_file in json_files:
            json_path = os.path.join(root, json_file)
            with open(json_path, mode='r', newline='', encoding='utf-8') as infile:
                json_data = json.load(infile)

                batch_size = 30
                process_transactions(URI, AUTH, DATABASE, json_data, batch_size, json_file, file_counter, file_total, address_cache)
                file_counter += 1

    end_time = time.time()
    total_time = end_time - start_time  # Calculate total runtime
    print(f'DONE. Total time taken: {total_time:.2f} seconds')