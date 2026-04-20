import time
import json
import csv
import logging
import os
import random
from confluent_kafka import Producer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "network-telemetry-stream"
CSV_FILE = "nsl-kdd-test.csv"

def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Message delivery failed: {err}")

def create_mock_csv():
    """Helper to generate a dummy dataset if original is not present."""
    headers = [f"feature_{i}" for i in range(1, 40)] + ["src_bytes", "dst_bytes", "label"]
    with open(CSV_FILE, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(headers)
        for _ in range(5000): # 5000 mock rows
            row = [round(random.uniform(0, 100), 2) for _ in range(41)]
            row.append("normal" if random.random() > 0.05 else "anomaly") # 5% anomalies
            writer.writerow(row)
    logger.info(f"Created mock {CSV_FILE} for testing with 5000 rows.")

def main():
    if not os.path.exists(CSV_FILE):
        logger.warning(f"File {CSV_FILE} not found. Generating mock data for simulation...")
        create_mock_csv()

    conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'client.id': 'python-producer'
    }
    
    producer = Producer(conf)
    logger.info(f"Connected to Kafka broker at {KAFKA_BROKER}")

    try:
        while True:
            with open(CSV_FILE, mode='r') as file:
                reader = csv.DictReader(file)
                for row_num, row in enumerate(reader):
                    parsed_row = {}
                    for k, v in row.items():
                        try:
                            parsed_row[k] = float(v)
                        except ValueError:
                            parsed_row[k] = v
                    
                    # Add pseudo fields for tracking
                    parsed_row["timestamp"] = time.time()
                    parsed_row["src_ip"] = f"192.168.1.{random.randint(1, 255)}"
                    
                    payload = json.dumps(parsed_row)
                    producer.produce(TOPIC, value=payload, callback=delivery_report)
                    
                    producer.poll(0)
                    
                    if row_num % 500 == 0:
                        logger.info(f"Produced {row_num} network telemetry messages.")
                    
                    # Simulate high velocity stream
                    time.sleep(0.01)
            logger.info("Reached end of CSV. Replaying baseline stream dynamically...")
                
    except KeyboardInterrupt:
        logger.info("Interrupted by user. Shutting down...")
    except Exception as e:
        logger.error(f"Producer failed: {e}")
    finally:
        logger.info("Flushing producer buffer before exit...")
        producer.flush()

if __name__ == "__main__":
    main()
