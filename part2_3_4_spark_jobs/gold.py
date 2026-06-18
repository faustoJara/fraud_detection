from pyspark.sql import SparkSession
from pyspark.sql.functions import col, array, lit, when, concat_ws, size, expr

def main():
    # 1. Configuración blindada
    spark = SparkSession.builder \
        .appName("GoldFraudDetection") \
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

    tabla_origen = "lakehouse.payments.silver_payments_parte3"
    tabla_alertas = "lakehouse.payments.fraud_alerts_parte4"
    tabla_relaciones = "lakehouse.payments.payments_relations_parte4"

    print(f"📖 Extrayendo datos desde {tabla_origen}...")
    df_silver = spark.read.table(tabla_origen)

    print("⚡ Evaluando matriz de riesgo probabilístico...")
    # 2. Asignación de motivos
    df_motivos = df_silver.withColumn(
        "lista_cruda",
        array(
            when(col("tx_by_card_5m") >= 5, lit("high_card_velocity_5m")),
            when(col("distinct_merchants_10m") >= 4, lit("many_merchants_10m")),
            when(col("distinct_countries_1h") >= 3, lit("many_countries_1h")),
            when(col("distinct_cards_per_device") >= 4, lit("device_shared_by_cards")),
            when(col("declined_ratio_1h") >= 0.45, lit("high_declined_ratio_1h")),
            when(col("amount") >= 1200, lit("high_amount"))
        )
    ).withColumn("reasons", expr("filter(lista_cruda, elemento -> elemento IS NOT NULL)")) \
     .drop("lista_cruda")

    # 3. Cálculo de Risk Score
    df_scoring = df_motivos.withColumn(
        "risk_score",
        when(col("tx_by_card_5m") >= 5, 24).otherwise(0) +
        when(col("distinct_merchants_10m") >= 4, 18).otherwise(0) +
        when(col("distinct_countries_1h") >= 3, 18).otherwise(0) +
        when(col("distinct_cards_per_device") >= 4, 16).otherwise(0) +
        when(col("declined_ratio_1h") >= 0.45, 12).otherwise(0) +
        when(col("amount") >= 1200, 12).otherwise(0)
    ).withColumn("reasons_text", concat_ws(", ", col("reasons")))

    # 4. Tabla de Alertas (solo transacciones de alto riesgo)
    print(f"🚀 Materializando Alertas en {tabla_alertas}...")
    df_alertas = df_scoring.filter(col("risk_score") >= 20).drop("reasons")
    df_alertas.writeTo(tabla_alertas).using("iceberg").createOrReplace()

    # 5. Tabla de Relaciones (todo, para el grafo)
    print(f"🚀 Materializando Grafo Relacional en {tabla_relaciones}...")
    df_relaciones = df_scoring.drop("reasons")
    df_relaciones.writeTo(tabla_relaciones).using("iceberg").createOrReplace()

    print("✅ Capa Gold completada. Pipeline de fraude 100% operativo.")
    spark.stop()

if __name__ == "__main__":
    main()