import json
from elasticsearch import Elasticsearch
from datetime import datetime

# Elasticsearch server details
es_host = '10.184.230.45'
es_port = 9200
es_index = 'hackathondata'
es_type = '_doc'  # Elasticsearch 7.x and later versions use "_doc" as the type

# Log file location
log_file_path = '/root/amar_hackathon/host-session-manager_log.log'

# Connect to Elasticsearch
es = Elasticsearch([{'host': es_host, 'port': es_port}])

def parse_log_line(line):
    # Customize this function based on your log format
    # This example assumes a simple JSON log format
    try:
        log_data = json.loads(line)
        print(log_data)
        return log_data
    except json.JSONDecodeError as e:
        print(f"Error parsing log line: {line}\nError: {e}")
        return None

def send_to_elasticsearch(log_data):
    if log_data:
        timestamp = datetime.now()
        try:
            es.index(index=es_index, doc_type=es_type, body=log_data, timestamp=timestamp)
            print(f"Log data sent to Elasticsearch: {log_data}")
        except Exception as e:
            print("Error")

# Read and process log file
with open(log_file_path, 'r') as log_file:
    for line in log_file:
        log_data = parse_log_line(line)
        send_to_elasticsearch(log_data)
