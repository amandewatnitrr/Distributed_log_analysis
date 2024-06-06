from __future__ import print_function
from confluent_kafka import Producer
import time

def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

def push_logs_to_kafka(file_path, topic, bootstrap_servers):
    producer_conf = {
        'bootstrap.servers': bootstrap_servers,
        'client.id': 'log-producer'
    }

    producer = Producer(producer_conf)

    try:
        with open(file_path, 'r') as file:
            for line in file:
                # Produce line to Kafka topic
                producer.produce(topic, line.rstrip(), callback=delivery_report)
                producer.poll(0)  # Trigger delivery report callbacks
                time.sleep(1)    # Sleep for 1 second

    except Exception as e:
        print('Error: {}'.format(e))

    finally:
        producer.flush()

if __name__ == "__main__":
    log_file_path = '/Users/akd/Github/aman_Hackathon_2024/logs.json'  # Adjust the path to your log file
    kafka_topic = 'my_logs'  # Replace with the actual Kafka topic
    kafka_bootstrap_servers = 'localhost:9092'  # Replace with your Kafka bootstrap servers

    push_logs_to_kafka(log_file_path, kafka_topic, kafka_bootstrap_servers)
