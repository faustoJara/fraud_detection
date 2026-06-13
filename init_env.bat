@echo off
echo ===================================================
echo Iniciando configuracion del entorno Lakehouse...
echo ===================================================

echo 1. Creando entorno Conda (lakehouse_env)...
call conda create -n lakehouse_env python=3.10 -y
call conda activate lakehouse_env

echo 2. Instalando dependencias base y herramientas de ML con soporte CUDA...
pip install kafka-python pyspark boto3 neo4j sqlalchemy pymysql pandas
pip install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu118
pip install transformers datasets accelerate

echo 3. Creando estructura de directorios...
mkdir dags part1_kafka_producer part2_3_4_spark_jobs part7_neo4j part8_9_10_scripts part12_ml_huggingface data\minio data\mysql

echo 4. Levantando contenedores Docker en background...
docker-compose up -d

echo ===================================================
echo Entorno listo. Accede a MinIO en localhost:9001
echo Accede a Kafka en localhost:9092
echo ===================================================
pause