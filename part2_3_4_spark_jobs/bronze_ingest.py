import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

def main():
    # 1. Configuración de la sesión conectada a Postgres y MinIO
    spark = SparkSession.builder \
        .appName("BronzeIngestion") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.kafka:kafka-clients:3.5.0,org.apache.iceberg:iceberg-aws-bundle:1.5.2") \
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.lakehouse.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog") \
        .config("spark.sql.catalog.lakehouse.uri", "jdbc:postgresql://postgres:5432/platform") \
        .config("spark.sql.catalog.lakehouse.warehouse", "s3a://lakehouse/warehouse") \
        .config("spark.sql.catalog.lakehouse.jdbc.user", "lakehouse") \
        .config("spark.sql.catalog.lakehouse.jdbc.password", "lakehouse") \
        .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO") \
        .config("spark.sql.catalog.lakehouse.s3.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minio") \
        .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # 2. Leer desde Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "pagos_topic") \
        .option("startingOffsets", "earliest") \
        .load()

    # 3. Definir el esquema para entender el JSON crudo
    payment_schema = StructType([
        StructField("event_time", StringType(), True),
        StructField("payment_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("card_id", StringType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("ip", StringType(), True),
        StructField("country", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("status", StringType(), True),
        StructField("mcc", StringType(), True)
    ])

    # 4. Transformación mínima: convertir el valor binario a JSON y expandir columnas
    bronze_df = kafka_df \
        .selectExpr("CAST(value AS STRING) as json_string") \
        .select(from_json(col("json_string"), payment_schema).alias("data")) \
        .select("data.*") \
        .withColumn("ingestion_timestamp", current_timestamp())

    # Garantizar que el namespace existe en Postgres
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.payments")

    # 5. Escribir el Stream en Iceberg (Capa Bronze)
    print("Iniciando ingesta hacia Iceberg Bronze...")
    query = bronze_df.writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .option("checkpointLocation", "s3a://lakehouse/checkpoints/bronze_events") \
        .toTable("lakehouse.payments.bronze_payments_parte2")

    query.awaitTermination()

if __name__ == "__main__":
    main()