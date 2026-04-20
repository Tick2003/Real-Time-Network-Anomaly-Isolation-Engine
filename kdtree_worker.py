import json
import logging
import os
import pickle
import redis
import numpy as np
import time
from confluent_kafka import Consumer, Producer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
IN_TOPIC = "network-telemetry-stream"
OUT_TOPIC = "anomaly-alerts"
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

r_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)

def load_models_from_redis():
    """Poll redis until models are available"""
    while True:
        try:
            tree_data = r_client.get("current_baseline_kdtree")
            tau_str = r_client.get("current_anomaly_threshold")
            if tree_data and tau_str:
                model_data = pickle.loads(tree_data)
                tau = float(tau_str)
                logger.info(f"Loaded KD-Tree and Target Threshold (tau={tau:.5f})")
                return model_data['tree'], model_data['scaler'], model_data['features'], tau
        except Exception as e:
            logger.error(f"Error loading from redis: {e}")
        logger.info("Awaiting Baseline KD-Tree construction in memory space...")
        time.sleep(2)

def main():
    logger.info("Initializing KD-Tree Core Worker Mechanism...")
    tree, scaler, feature_cols, tau = load_models_from_redis()
    
    # Producer for dispatching Outlier detections
    producer = Producer({'bootstrap.servers': KAFKA_BROKER, 'client.id': 'kdtree-worker-pub'})
    
    # Consumer for the fast-velocity telemetry stream
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'anomaly-detector-group',
        'auto.offset.reset': 'latest' # Real-time behavior: handle newest logs
    })
    
    consumer.subscribe([IN_TOPIC])
    logger.info(f"Worker bound to '{IN_TOPIC}'. Polling flows sub-5ms...")

    messages_processed = 0
    anomalies_detected = 0

    try:
        while True:
            msg = consumer.poll(0.5)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Stream Error: {msg.error()}")
                continue
                
            try:
                payload = json.loads(msg.value().decode('utf-8'))
                
                # Reconstruction of strictly matching numerical metrics vector
                vector = []
                valid_flow = True
                for col in feature_cols:
                    if col in payload:
                        vector.append(float(payload[col]))
                    else:
                        valid_flow = False
                        break
                        
                if not valid_flow:
                    continue
                    
                X_new = np.array([vector])
                
                # Transform mapping into [0, 1] relative metric bounds
                X_scaled = scaler.transform(X_new)
                
                # Nearest Neighbors distance calc via mathematical O(log N) operations
                # k=5 directly since this is an out-of-sample data point
                distances, indices = tree.query(X_scaled, k=5)
                mean_dist = np.mean(distances[0])
                
                messages_processed += 1
                
                # Evaluates point divergence geometry strictly by thresholding
                if mean_dist > tau:
                    anomalies_detected += 1
                    alert = {
                        "timestamp": payload.get("timestamp", time.time()),
                        "src_ip": payload.get("src_ip", "0.0.0.0"),
                        "anomaly_score": float(mean_dist),
                        "threshold": float(tau),
                        "severity": "CRITICAL" if mean_dist > tau * 2.0 else "WARNING"
                    }
                    
                    # Immediately dispatch detection events to central queue pipeline
                    producer.produce(OUT_TOPIC, value=json.dumps(alert))
                    producer.poll(0)
                
                if messages_processed % 500 == 0:
                    logger.info(f"Worker stats - Processed: {messages_processed}, Anomalies isolated: {anomalies_detected}")
                    
            except Exception as e:
                logger.warning(f"Discarding invalid flow structure: {e}")
                
    except KeyboardInterrupt:
        logger.info("Real-Time Engine halted manually.")
    finally:
        consumer.close()
        producer.flush()

if __name__ == "__main__":
    main()
