# ML Training Dockerfile
FROM python:3.9-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy ML code
COPY donor-analytics-enterprise/ml /app/ml

# Set environment variables
ENV PYTHONPATH=/app
ENV MODEL_DIR=/app/models

# Default command - can be overridden
CMD ["python", "/app/ml/train_xgb.py"]