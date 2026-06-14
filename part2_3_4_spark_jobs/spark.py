import os
import sys

# 1. Configuración de rutas
os.environ['JAVA_HOME'] = r'C:\Users\faust\miniconda3\envs\lakehouse_env\Library'
os.environ['HADOOP_HOME'] = r'C:\hadoop'
os.environ['PYSPARK_PYTHON'] = sys.executable

# 2. DEBUG: Verificar que las rutas existen
print(f"DEBUG: JAVA_HOME es: {os.environ['JAVA_HOME']}")
java_exe = os.path.join(os.environ['JAVA_HOME'], 'bin', 'java.exe')
print(f"DEBUG: Existe java.exe en {java_exe}? {os.path.exists(java_exe)}")

print(f"DEBUG: HADOOP_HOME es: {os.environ['HADOOP_HOME']}")
winutils = os.path.join(os.environ['HADOOP_HOME'], 'bin', 'winutils.exe')
print(f"DEBUG: Existe winutils.exe en {winutils}? {os.path.exists(winutils)}")

# 3. Asegurar PATH
os.environ['PATH'] = os.path.join(os.environ['HADOOP_HOME'], 'bin') + os.pathsep + os.environ['PATH']

from pyspark.sql import SparkSession

# Spark con los jars necesarios para Kafka e Iceberg
spark = SparkSession.builder \
    .appName("KafkaToIcebergBronze") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "s3a://bronze/warehouse") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# Leer desde Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "pagos_topic") \
    .load()

# Convertir el valor binario de Kafka a string y procesar el esquema
# Recuerda que el documento pide no realizar transformaciones complejas en esta etapa [cite: 169]
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StringType

# Simplemente cast a string para guardar "tal cual llegan"
events = df.selectExpr("CAST(value AS STRING)")

# Escribir en Iceberg (Bronze)
query = events.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .option("path", "local.db.bronze_pagos") \
    .option("checkpointLocation", "s3a://bronze/checkpoints/bronze_pagos") \
    .start()

query.awaitTermination()