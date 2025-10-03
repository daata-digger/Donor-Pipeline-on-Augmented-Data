"""
Pipeline Overview Dashboard
Shows end-to-end data pipeline status including:
- Data Ingestion (Sources, Volume, Freshness)
- Data Processing (ETL Status, Quality Checks)
- Warehouse Status (Snowflake Metrics)
- ML Pipeline Health
- Visualization Layer Status
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import pathlib

st.set_page_config(page_title="Pipeline Overview", layout="wide")

# Helper functions for pipeline metrics
def get_ingestion_metrics():
    """Mock ingestion metrics"""
    return {
        "salesforce_npsp": {
            "status": "Healthy",
            "last_sync": "10 minutes ago",
            "records": "1.2M donors",
            "latency": "2.5 mins"
        },
        "iwave": {
            "status": "Warning",
            "last_sync": "1 hour ago",
            "records": "800K profiles",
            "latency": "15 mins"
        },
        "irs_990": {
            "status": "Healthy",
            "last_sync": "1 day ago",
            "records": "2.1M filings",
            "latency": "30 mins"
        }
    }

def get_pipeline_status():
    """Mock pipeline status"""
    return {
        "ingestion_dag": {
            "status": "Success",
            "runtime": "45 mins",
            "last_run": "2 hours ago",
            "success_rate": "98.5%"
        },
        "entity_resolution": {
            "status": "Success",
            "runtime": "30 mins",
            "matches_found": "50K",
            "accuracy": "99.2%"
        },
        "dbt_transformations": {
            "status": "Success",
            "runtime": "15 mins",
            "models": "25/25 passed",
            "test_coverage": "92%"
        }
    }

def get_warehouse_metrics():
    """Mock Snowflake metrics"""
    return {
        "credits_used": "150",
        "storage_tb": "2.5",
        "active_queries": "25",
        "query_performance": "95% under 10s"
    }

def get_ml_metrics():
    """Mock ML pipeline metrics"""
    return {
        "model_health": {
            "status": "Healthy",
            "drift": "No drift detected",
            "accuracy": "0.85",
            "last_retrain": "7 days ago"
        },
        "predictions": {
            "volume": "50K donors/day",
            "latency": "200ms avg",
            "success_rate": "99.9%"
        }
    }

# Main Pipeline Overview
st.title("Pipeline Overview")

# Data Sources & Ingestion
st.header("Data Sources & Ingestion")
ingestion_metrics = get_ingestion_metrics()

source_cols = st.columns(len(ingestion_metrics))
for col, (source, metrics) in zip(source_cols, ingestion_metrics.items()):
    with col:
        st.subheader(source)
        status_color = "🟢" if metrics["status"] == "Healthy" else "🟡"
        st.write(f"{status_color} Status: {metrics['status']}")
        st.write(f"Last Sync: {metrics['last_sync']}")
        st.write(f"Records: {metrics['records']}")
        st.write(f"Latency: {metrics['latency']}")

# Pipeline Status
st.header("⚙️ Processing Pipeline")
pipeline_status = get_pipeline_status()

# Create pipeline flow diagram using Plotly
fig = go.Figure(data=[
    go.Bar(
        x=[100, 80, 60],
        y=['Data Ingestion', 'Processing', 'ML Pipeline'],
        orientation='h',
        text=['Complete', 'In Progress', 'Pending'],
        textposition='auto',
    )
])

fig.update_layout(
    title='Pipeline Progress',
    xaxis_title='Completion %',
    yaxis_title='Pipeline Stage'
)

st.plotly_chart(fig, use_container_width=True)

# Pipeline Metrics
pipeline_cols = st.columns(len(pipeline_status))
for col, (stage, metrics) in zip(pipeline_cols, pipeline_status.items()):
    with col:
        st.subheader(stage.replace('_', ' ').title())
        status_color = "🟢" if metrics["status"] == "Success" else "🟡"
        st.write(f"{status_color} Status: {metrics['status']}")
        for key, value in metrics.items():
            if key != "status":
                st.write(f"{key.replace('_', ' ').title()}: {value}")

# Data Warehouse Status
st.header("🏢 Data Warehouse")
warehouse_metrics = get_warehouse_metrics()

warehouse_cols = st.columns(len(warehouse_metrics))
for col, (metric, value) in zip(warehouse_cols, warehouse_metrics.items()):
    with col:
        st.metric(
            label=metric.replace('_', ' ').title(),
            value=value
        )

# ML Pipeline Health
st.header("🤖 ML Pipeline")
ml_metrics = get_ml_metrics()

ml_col1, ml_col2 = st.columns(2)

with ml_col1:
    st.subheader("Model Health")
    for key, value in ml_metrics["model_health"].items():
        st.write(f"{key.replace('_', ' ').title()}: {value}")

with ml_col2:
    st.subheader("Prediction Service")
    for key, value in ml_metrics["predictions"].items():
        st.write(f"{key.replace('_', ' ').title()}: {value}")

# Data Quality
st.header("Data Quality Checks")

# Mock data quality metrics
quality_metrics = pd.DataFrame({
    'Metric': ['Completeness', 'Accuracy', 'Timeliness', 'Consistency'],
    'Score': [95, 98, 92, 96]
})

fig = px.bar(quality_metrics, x='Metric', y='Score',
             title='Data Quality Metrics',
             labels={'Score': 'Quality Score (%)'},
             range_y=[0, 100])
st.plotly_chart(fig, use_container_width=True)

# Pipeline Steps Table
st.header("🔍 Pipeline Steps")

steps_df = pd.DataFrame({
    'Step': [
        'Raw Data Ingestion',
        'Entity Resolution',
        'Data Transformation',
        'Feature Engineering',
        'Model Training',
        'Prediction Service'
    ],
    'Status': ['Complete', 'Complete', 'Complete', 'Running', 'Pending', 'Pending'],
    'Duration': ['45m', '30m', '15m', '10m', '-', '-'],
    'Records': ['1.2M', '800K', '750K', '700K', '-', '-']
})

st.dataframe(steps_df, use_container_width=True)

# Refresh Rate
st.sidebar.title("Refresh Settings")
refresh_rate = st.sidebar.slider(
    "Dashboard Refresh Rate (minutes)",
    min_value=1,
    max_value=60,
    value=5
)

st.sidebar.info(f"Dashboard auto-refreshes every {refresh_rate} minutes")

# Add last updated timestamp
st.sidebar.write("Last Updated:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))