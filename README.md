# High-Throughput Network Anomaly Isolation Engine

This repository contains a full production-ready real-time cybersecurity pipeline that detects network intrusions at extremely low latency (sub-5ms) without Deep Learning algorithms. It strictly utilizes geometrical mapping inside dimensional **Metric Spaces** mapped iteratively through dense **KD-Trees**. 

## Tech Stack
- **Infrastructure**: Docker & Docker Compose
- **Data Streaming Engine**: Apache Kafka & Zookeeper (confluent-kafka)
- **Fast Access Distributed Memory**: Redis
- **Math & Isolation Processing Algorithm**: Python 3.11, Scikit-Learn `KDTree`, Numpy
- **Visualization WebApp Component**: Streamlit (Pandas)

## Internal Architecture
1. **Kafka Telemetry Hub (`kafka_producer.py`)**: Reads simulated telemetry traffic points locally (`nsl-kdd-test.csv`), automatically injecting synthetic mappings if no file is present. Rapid streams are stored directly into Kafka `network-telemetry-stream`.
2. **Offline Isolation Trainer (`baseline_builder.py`)**: Models safe/secure system behaviors. Pre-scales mathematical representations using `MinMaxScaler` and generates $O(N \log N)$ initial building calculations over standard KD-Tree geometries. Parameters and the pickled Tree are stored immediately to Redis.
3. **Core KD-Tree Workers (`kdtree_worker.py`)**: The low latency loop mapping relative Euclidean limits through fast queries in established spaces. Calculations of distances vs threshold $\tau$ evaluate threats in theoretical $O(\log N)$ thresholds over the stored matrix nodes. Overflows are pushed continuously to `anomaly-alerts`.
4. **Threat Dashboard (`frontend_app.py`)**: Subscribes directly against `anomaly-alerts` mapping active thresholds through live-refresh auto-plotting visual blocks.

## Execution

### 1. Build and Run the Complete Docker Engine Hub
The system is constructed so you can rapidly iterate and launch the pipeline completely isolated.

```bash
docker-compose up -d --build
```
*(This launches Zookeeper, Redis, Kafka brokers, and initiates every worker Python component seamlessly directly in the background).*

### 2. View Visualizations Real-Time
Observe point breaches propagating into the platform:
Navigate to [http://localhost:8501](http://localhost:8501) on your local browser. 

The Dashboard polls continuously against Kafka. Because we wrote auto-creation fallback modules for NSL-KDD inside the producer, **the pipeline automatically works natively and generates point-traffic on the fly instantly upon spinning up the compose.**

### Validation Breakdown Outline

1. Observe container health using `docker-compose ps`
2. You can trail Kafka telemetry messages using:
`docker-compose logs -f kafka_producer`
3. Analyze KD-Tree calculation metrics mapping dynamically to logs with:
`docker-compose logs -f baseline_builder kdtree_worker`
