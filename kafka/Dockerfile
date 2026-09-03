FROM python:3.11-slim
WORKDIR /app
COPY kafka/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir lz4
COPY cassandra/cassandra_client.py ./cassandra_client.py
COPY kafka/kafka_producer.py .
CMD ["python", "kafka_producer.py"]
COPY cassandra/cassandra_init.py ./cassandra_init.py
