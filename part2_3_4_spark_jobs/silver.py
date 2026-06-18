from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, to_timestamp, size, collect_list, array_distinct, avg, when, round as spark_round

def main():
    # 1. Configuración blindada (Idéntica a Bronze)
    spark = SparkSession.builder \
        .appName("SilverEnrichment") \
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.lakehouse.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog") \
        .config("spark.sql.catalog.lakehouse.uri", "jdbc:postgresql://postgres:5432/platform") \
        .config("spark.sql.catalog.lakehouse.warehouse", "s3a://lakehouse/warehouse") \
        .config("spark.sql.catalog.lakehouse.jdbc.user", "lakehouse") \
        .config("spark.sql.catalog.lakehouse.jdbc.password", "lakehouse") \
        .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minio") \
        .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
            .config("spark.sql.catalog.lakehouse.jdbc.schema-version", "V1") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    tabla_origen = "lakehouse.payments.bronze_payments_parte2"
    tabla_destino = "lakehouse.payments.silver_payments_parte3"

    print(f"📖 Leyendo datos crudos desde {tabla_origen}...")
    df_bronze = spark.read.table(tabla_origen)

    # 2. Limpieza y Tipado
    df_tipado = df_bronze \
        .withColumn("event_time_ts", to_timestamp(col("event_time"))) \
        .filter(col("event_time_ts").isNotNull() & col("payment_id").isNotNull()) \
        .dropDuplicates(["payment_id"])

    # 3. Ventanas Temporales para vectorización de comportamiento
    marcador_tiempo = col("event_time_ts").cast("long")
    ventana_tarjeta_5min = Window.partitionBy("card_id").orderBy(marcador_tiempo).rangeBetween(-300, 0)
    ventana_tarjeta_10min = Window.partitionBy("card_id").orderBy(marcador_tiempo).rangeBetween(-600, 0)
    ventana_tarjeta_1hora = Window.partitionBy("card_id").orderBy(marcador_tiempo).rangeBetween(-3600, 0)
    ventana_dispositivo_global = Window.partitionBy("device_id")

    print("⚡ Calculando variables de comportamiento (ventanas temporales)...")
    df_silver = df_tipado \
        .withColumn("tx_by_card_5m", size(collect_list("payment_id").over(ventana_tarjeta_5min))) \
        .withColumn("distinct_merchants_10m", size(array_distinct(collect_list("merchant_id").over(ventana_tarjeta_10min)))) \
        .withColumn("distinct_countries_1h", size(array_distinct(collect_list("country").over(ventana_tarjeta_1hora)))) \
        .withColumn("distinct_cards_per_device", size(array_distinct(collect_list("card_id").over(ventana_dispositivo_global)))) \
        .withColumn("declined_ratio_1h", spark_round(avg(when(col("status") == "declined", 1.0).otherwise(0.0)).over(ventana_tarjeta_1hora), 4))

    # Seleccionar columnas finales
    df_final = df_silver.select(
        col("event_time_ts").alias("event_time"), "payment_id", "customer_id", "card_id", "merchant_id", "device_id",
        "ip", "country", "amount", "currency", "status", "mcc",
        "tx_by_card_5m", "distinct_merchants_10m", "distinct_countries_1h", "distinct_cards_per_device", "declined_ratio_1h"
    )

    print(f"🚀 Escribiendo datos enriquecidos en {tabla_destino}...")
    df_final.writeTo(tabla_destino).using("iceberg").createOrReplace()
    
    print("✅ Capa Silver completada con éxito.")
    spark.stop()

if __name__ == "__main__":
    main()