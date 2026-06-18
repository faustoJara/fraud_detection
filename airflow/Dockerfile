FROM apache/airflow:2.10.5-python3.11

USER airflow

# Instalación directa de las dependencias nativas para Trino y Neo4j
RUN pip install --no-cache-dir \
    trino==0.330.0 \
    neo4j==5.26.0