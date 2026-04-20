import streamlit as st
import pandas as pd
import json
import os
import time
from confluent_kafka import Consumer, KafkaException

KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "anomaly-alerts"

st.set_page_config(page_title="Network Anomaly Dashboard", layout="wide")
st.title("🛡️ Real-Time Network Anomaly Isolation Engine")
st.markdown("Monitoring the `anomaly-alerts` Kafka topic for isolated threats using Distributed KDE / Metric Spaces.")

# Initialize session state for storing alerts incrementally
if 'alerts_data' not in st.session_state:
    st.session_state['alerts_data'] = []

import uuid

@st.cache_resource
def get_kafka_consumer():
    conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': f'streamlit-dash-{uuid.uuid4()}',
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(conf)
    consumer.subscribe([TOPIC])
    return consumer

consumer = get_kafka_consumer()

# Consume messages with a very fast timeout to prevent UI blocking/flashing
msgs = consumer.consume(num_messages=50, timeout=0.05)

for msg in msgs:
    if msg.error():
        if msg.error().code() != KafkaException._PARTITION_EOF:
            st.error(f"Kafka Stream Error: {msg.error()}")
    else:
        payload = json.loads(msg.value().decode('utf-8'))
        st.session_state['alerts_data'].append(payload)

# Throttle the data size in memory to the last 150 alerts
if len(st.session_state['alerts_data']) > 150:
    st.session_state['alerts_data'] = st.session_state['alerts_data'][-150:]

if st.session_state['alerts_data']:
    df = pd.DataFrame(st.session_state['alerts_data'])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
    
    total_alerts = len(st.session_state['alerts_data'])
    critical_alerts = len(df[df['severity'] == "CRITICAL"])
    
    metrics_col1, metrics_col2 = st.columns(2)
    with metrics_col1:
        st.metric(label="Total Anomalies Tracked (Window)", value=total_alerts)
    with metrics_col2:
        st.metric(label="Critical System Incidents", value=critical_alerts, delta_color="inverse")
        
    st.subheader("Live Threat Event Mapping Log")
    
    def color_rules(val):
        color = 'red' if val == 'CRITICAL' else 'orange' if val == 'WARNING' else ''
        return f'color: {color}'
    
    st.dataframe(
        df.sort_values(by="timestamp", ascending=False).style.applymap(color_rules, subset=['severity']), 
        use_container_width=True
    )
        
    st.subheader("Distance Separation Threshold Scores - Scatter Geometry")
    st.scatter_chart(data=df, x='timestamp', y='anomaly_score', color='severity')

# Keep the streamlit thread constantly refreshing
time.sleep(1.0)
st.rerun()
