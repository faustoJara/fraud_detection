import os
import json
from uuid import uuid4
from datetime import datetime
from dateutil import parser
import boto3
from botocore.exceptions import ClientError
from trino.dbapi import connect
from dotenv import load_dotenv

load_dotenv()

# ==============================================================================
# CONFIGURACIÓN DE CONEXIÓN
# ==============================================================================
TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = int(os.getenv("TRINO_PORT", 9080))
TRINO_CATALOG = os.getenv("TRINO_CATALOG", "lakehouse")
TRINO_SCHEMA = os.getenv("TRINO_SCHEMA", "payments")
SOURCE_TABLE = os.getenv("SOURCE_TABLE", "gold_fraud_alerts_parte4")
USE_MINIO = os.getenv("USE_MINIO", "true").lower() == "true"
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET_NAME = os.getenv("S3_BUCKET", "lakehouse")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:9000") if USE_MINIO else None

# ==============================================================================
# TRINO
# ==============================================================================
def fetch_gold_fraud_alerts():
    print(f"🔌 Conectando a Trino {TRINO_HOST}:{TRINO_PORT}...")
    conn = connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user="analytics_user",
        catalog=TRINO_CATALOG,
        schema=TRINO_SCHEMA
    )
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {TRINO_CATALOG}.{TRINO_SCHEMA}.{SOURCE_TABLE}")
    rows = cursor.fetchall()
    if not rows:
        return []
    cols = [d[0] for d in cursor.description]
    print(f"📊 Registros extraídos: {len(rows)}")
    return [dict(zip(cols, r)) for r in rows]

# ==============================================================================
# S3 ACCESS
# ==============================================================================
def create_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        aws_session_token=AWS_SESSION_TOKEN,
        region_name=AWS_REGION,
        endpoint_url=S3_ENDPOINT
    )

# ==============================================================================
# BUCKET CREATE
# ==============================================================================
def ensure_bucket(s3_client):
    try:
        s3_client.head_bucket(Bucket=S3_BUCKET_NAME)
        print(f"✅ Bucket OK: {S3_BUCKET_NAME}")
        return
    except ClientError:
        print(f"📁 Creando bucket: {S3_BUCKET_NAME}")
        if AWS_REGION == "us-east-1":
            s3_client.create_bucket(Bucket=S3_BUCKET_NAME)
        else:
            s3_client.create_bucket(
                Bucket=S3_BUCKET_NAME,
                CreateBucketConfiguration={"LocationConstraint": AWS_REGION}
            )

# ==============================================================================
# UPLOAD
# ==============================================================================
def upload_alerts_to_s3(alerts):
    print("☁️ Inicializando S3...")
    s3_client = create_s3_client()
    ensure_bucket(s3_client)
    partitions = {}
    for a in alerts:
        ts = a.get("event_time")
        if not ts:
            continue
        try:
            dt = ts if isinstance(ts, datetime) else parser.parse(str(ts))
        except:
            continue
        key = (
            dt.strftime("%Y"),
            dt.strftime("%m"),
            dt.strftime("%d"),
            dt.strftime("%H"),
            dt.strftime("%M")
        )
        partitions.setdefault(key, []).append(a)
    print(f"🧩 Particiones generadas: {len(partitions)}")
    for (y, m, d, h, mi), rows in partitions.items():
        filename = f"{uuid4().hex}.json"
        s3_key = f"gold/year={y}/month={m}/day={d}/hour={h}/minute={mi}/{filename}"
        body = json.dumps(rows, default=str).encode("utf-8")
        try:
            print(f"🚀 Upload -> s3://{S3_BUCKET_NAME}/{s3_key}")
            s3_client.put_object(
                Bucket=S3_BUCKET_NAME,
                Key=s3_key,
                Body=body,
                ContentType="application/json"
            )
        except Exception as e:
            print(f"❌ Error subiendo {s3_key}: {str(e)}")

# ==============================================================================
# MAIN
# ==============================================================================
if __name__ == "__main__":
    try:
        data = fetch_gold_fraud_alerts()
        if data:
            upload_alerts_to_s3(data)
        else:
            print("⚠️ Sin datos en GOLD")
    except Exception as e:
        print(f"💥 ERROR GENERAL: {str(e)}")