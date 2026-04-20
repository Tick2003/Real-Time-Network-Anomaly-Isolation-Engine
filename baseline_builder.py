import pandas as pd
import numpy as np
import pickle
import redis
import logging
import os
from sklearn.neighbors import KDTree
from sklearn.preprocessing import MinMaxScaler

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
CSV_FILE = "nsl-kdd-test.csv"

def wait_for_redis(client):
    import time
    for _ in range(15):
        try:
            if client.ping():
                return True
        except redis.ConnectionError:
            logger.warning("Waiting for Redis to become healthy...")
            time.sleep(2)
    return False

def main():
    if not os.path.exists(CSV_FILE):
        logger.error(f"Cannot build baseline. {CSV_FILE} not found. Run kafka_producer.py first to generate it.")
        return

    logger.info("Reading dataset for KD-Tree offline training...")
    df = pd.read_csv(CSV_FILE)
    
    # Baseline relies purely on "Normal" network traffic
    if 'label' in df.columns:
        normal_df = df[df['label'] == 'normal'].copy()
    else:
        normal_df = df.copy()
        
    # Extract numerical features to formulate the geometrical point spaces
    numeric_cols = normal_df.select_dtypes(include=[np.number]).columns.tolist()
    
    # Exclude system fields
    for col in ['timestamp', 'src_ip', 'Unnamed: 0']:
        if col in numeric_cols:
            numeric_cols.remove(col)
            
    X_normal = normal_df[numeric_cols].values
    
    if len(X_normal) == 0:
        logger.error("No numerical normal data found to build baseline.")
        return
        
    logger.info(f"Training KD-Tree Model on shape: {X_normal.shape}")
    
    # 1. Normalize Vector Space using Min-Max Scaling [0, 1]
    scaler = MinMaxScaler()
    X_scaled = scaler.fit_transform(X_normal)
    
    # 2. Build KD-Tree - Building Time: O(N log N)
    logger.info("Initializing KD-Tree (leaf_size=40, metric='euclidean')...")
    tree = KDTree(X_scaled, leaf_size=40, metric='euclidean')
    logger.info("KD-Tree built successfully.")
    
    # 3. Calculate k=5 nearest neighbors distances for determining the dynamic threshold
    # Since we query the training points themselves, the 1st neighbor is the point itself (distance 0.0)
    # Thus we query for k=6 to get 5 actual distinct neighbors.
    logger.info("Approximating k=5 nearest neighbors across baseline...")
    distances, indices = tree.query(X_scaled, k=6)
    
    # Slice [:, 1:] to exclude the zeroth self-distance
    mean_distances = np.mean(distances[:, 1:], axis=1)
    
    # 4. Extract Gaussian metrics
    mu = np.mean(mean_distances)
    sigma = np.std(mean_distances)
    
    # Define Anomaly Threshold as Tau = mu + 3*sigma (99.7% Empirical Rule / Chebyshev bounds)
    tau = mu + 3 * sigma
    logger.info(f"Calculated Baseline mu = {mu:.6f}, sigma = {sigma:.6f}")
    logger.info(f"Determined Anomaly Threshold (tau) = {tau:.6f}")
    
    # 5. Serialize Tree and threshold for Redis
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
    if not wait_for_redis(r):
        logger.error("Could not connect to Redis inside Baseline Builder.")
        return
        
    model_data = {
        'tree': tree,
        'scaler': scaler,
        'features': numeric_cols
    }
    
    logger.info("Serializing structure to Memory Store...")
    serialized_model = pickle.dumps(model_data)
    
    # Store KDTree Model & Threshold globally
    r.set("current_baseline_kdtree", serialized_model)
    r.set("current_anomaly_threshold", float(tau))
    
    logger.info("Model and Threshold successfully published to Redis.")

if __name__ == "__main__":
    main()
