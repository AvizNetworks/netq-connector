FROM python:3.9-slim

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy entrypoint and all collector modules
COPY src/collector/netq-collector.py ./netq-collector.py
COPY src/collector ./collector

# Run the NetQ Collector
CMD ["python", "netq-collector.py"]
