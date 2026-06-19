import os
import time
import boto3
from dotenv import load_dotenv

load_dotenv()

# ==========================================================
# CONFIGURACIÓN
# ==========================================================
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET = os.getenv("S3_BUCKET", "lakehouse-fausto-sdb-2026")
ATHENA_DB = "fraud_analytics_db"

# Rutas de S3 requeridas por Athena
S3_DATA_LOCATION = f"s3://{S3_BUCKET}/analytics/"
S3_OUTPUT_LOCATION = f"s3://{S3_BUCKET}/athena-results/"

# Inicializar cliente de Athena
athena = boto3.client(
    "athena", aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY,
    aws_session_token=AWS_SESSION_TOKEN, region_name=AWS_REGION
)

# ==========================================================
# GESTOR DE QUERIES (ESPERA SINCRÓNICA)
# ==========================================================
def run_athena_query(query, description):
    print(f"🏃‍♂️ Ejecutando: {description}...")
    try:
        response = athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": "default"},
            ResultConfiguration={"OutputLocation": S3_OUTPUT_LOCATION}
        )
        query_id = response["QueryExecutionId"]
        
        while True:
            status = athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]["Status"]["State"]
            if status in ["SUCCEEDED", "FAILED", "CANCELLED"]:
                break
            time.sleep(1)
            
        if status == "SUCCEEDED":
            print(f"   ✔️ Completada con éxito. Query ID: {query_id}")
            return query_id
        else:
            reason = athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]["Status"].get("StateChangeReason", "Desconocida")
            print(f"   ❌ Falló: {reason}")
            return None
    except Exception as e:
        print(f"   💥 Error de conexión: {str(e)}")
        return None

# ==========================================================
# PIPELINE ANALÍTICO PRINCIPAL
# ==========================================================
if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("📝 INICIANDO PROCESAMIENTO ANALÍTICO CON AMAZON ATHENA")
    print("=" * 60)

    # 1. Crear Base de Datos
    db_query = f"CREATE DATABASE IF NOT EXISTS {ATHENA_DB};"
    run_athena_query(db_query, f"Creación de Base de Datos ({ATHENA_DB})")

    # 2. Recrear Tabla Externa mapeando las columnas reales del JSON/CSV consolidado
    # Se añade la estructura exacta de tu capa Gold incluyendo puntuacion_riesgo y risk_score
    drop_query = f"DROP TABLE IF EXISTS {ATHENA_DB}.insights_report;"
    run_athena_query(drop_query, "Borrando tabla externa previa si existía")

    ddl_query = f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {ATHENA_DB}.insights_report (
            payment_id STRING,
            event_time STRING,
            customer_id STRING,
            card_id STRING,
            merchant_id STRING,
            device_id STRING,
            country STRING,
            amount DOUBLE,
            currency STRING,
            status STRING,
            mcc STRING,
            tx_by_card_5m INT,
            distinct_merchants_10m INT,
            distinct_countries_1h INT,
            distinct_cards_per_device INT,
            declined_ratio_1h DOUBLE,
            puntuacion_riesgo INT,
            reasons STRING,
            reasons_text STRING,
            device_type STRING,
            risk_score DOUBLE
        )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        STORED AS TEXTFILE
        LOCATION '{S3_DATA_LOCATION}'
        TBLPROPERTIES ('skip.header.line.count'='1');
    """
    run_athena_query(ddl_query, f"Creación de Tabla Externa Real ({ATHENA_DB}.insights_report)")

    # 3. Consulta de Negocio 1: Detección de Alto Riesgo (Buscando puntuacion_riesgo > 10)
    q1 = f"""
        SELECT payment_id, customer_id, amount, puntuacion_riesgo, country, event_time
        FROM {ATHENA_DB}.insights_report
        WHERE puntuacion_riesgo > 10 OR status = 'declined'
        ORDER BY amount DESC, puntuacion_riesgo DESC
        LIMIT 50;
    """
    run_athena_query(q1, "Consulta 1/3 -> Transacciones de Alto Riesgo")

    # 4. Consulta de Negocio 2: Actividad Sospechosa de Usuarios / Tarjetas
    q2 = f"""
        SELECT customer_id, COUNT(payment_id) AS total_transacciones, AVG(amount) AS monto_promedio,
               SUM(CASE WHEN status = 'declined' THEN 1 ELSE 0 END) AS alertas_confirmadas, MAX(puntuacion_riesgo) AS max_puntuacion
        FROM {ATHENA_DB}.insights_report
        GROUP BY customer_id
        HAVING COUNT(payment_id) > 1 OR MAX(puntuacion_riesgo) > 10
        ORDER BY alertas_confirmadas DESC, total_transacciones DESC;
    """
    run_athena_query(q2, "Consulta 2/3 -> Actividad Sospechosa de Usuarios")

    # 5. Consulta de Negocio 3: Patrones Geográficos de Fraude
    q3 = f"""
        SELECT country, COUNT(payment_id) AS volumen, SUM(amount) AS capital_en_riesgo, AVG(puntuacion_riesgo) AS riesgo_medio,
               ROUND(SUM(CASE WHEN status = 'declined' THEN 1.0 ELSE 0.0 END) / COUNT(payment_id) * 100, 2) AS tasa_fraude_pct
        FROM {ATHENA_DB}.insights_report
        GROUP BY country
        ORDER BY capital_en_riesgo DESC, tasa_fraude_pct DESC;
    """
    run_athena_query(q3, "Consulta 3/3 -> Análisis de Patrones Geográficos")

    print("\n🎉 === PIPELINE DE ATHENA FINALIZADO CON ÉXITO ===")
    print(f"💾 Todos los CSVs resultantes se han guardado en: {S3_OUTPUT_LOCATION}\n")