import os
import json
import boto3
import pymysql
from datetime import datetime
from uuid import uuid4
from trino.dbapi import connect
from dotenv import load_dotenv

load_dotenv()

# ==========================================================
# CONFIG
# ==========================================================
TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = int(os.getenv("TRINO_PORT", 9080))
TRINO_CATALOG = os.getenv("TRINO_CATALOG", "iceberg")
TRINO_SCHEMA = os.getenv("TRINO_SCHEMA", "payments")
SOURCE_TABLE = "silver_payments_parte3"
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET = os.getenv("S3_BUCKET")
RDS_IDENTIFIER = os.getenv("RDS_IDENTIFIER", "fraud-mysql-db-parte9")
SG_NAME = "fraud-rds-public-sg"

# ==========================================================
# 1. EXTRACCIÓN SILVER (TRINO)
# ==========================================================
def fetch_silver():
    print("\n" + "=" * 60)
    print("🔌 ETAPA 1/4 - EXTRACCIÓN DESDE TRINO (SILVER)")
    print("=" * 60)
    conn = connect(
        host=TRINO_HOST, port=TRINO_PORT, user="analytics_user",
        catalog=TRINO_CATALOG, schema=TRINO_SCHEMA
    )
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {TRINO_CATALOG}.{TRINO_SCHEMA}.{SOURCE_TABLE}")
    rows = cursor.fetchall()
    if not rows:
        print("⚠️ No hay datos en Silver")
        return []
    cols = [d[0] for d in cursor.description]
    data = [dict(zip(cols, r)) for r in rows]
    print(f"📊 Registros extraídos: {len(data)}")
    print("Base de datos destino configurada en memoria.")
    return data

# ==========================================================
# 2. SUBIDA A S3 (STAGING SILVER)
# ==========================================================
def upload_silver_to_s3(records):
    print("\n" + "=" * 60)
    print("☁️ ETAPA 2/4 - STAGING EN S3 (SILVER)")
    print("=" * 60)
    s3 = boto3.client(
        "s3", aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        aws_session_token=AWS_SESSION_TOKEN, region_name=AWS_REGION
    )
    partitions = {}
    print("🧠 Particionando datos por fecha...")
    for r in records:
        ts = r.get("event_time")
        if not ts:
            continue
        dt = ts if isinstance(ts, datetime) else datetime.fromisoformat(str(ts).replace("Z", ""))
        key = (dt.strftime("%Y"), dt.strftime("%m"), dt.strftime("%d"))
        partitions.setdefault(key, []).append(r)
    print(f"🧩 Particiones creadas: {len(partitions)}")
    total_uploaded = 0
    for (y, m, d), rows in partitions.items():
        filename = f"{uuid4().hex}.json"
        s3_key = f"silver/payments/year={y}/month={m}/day={d}/{filename}"
        body = json.dumps(rows, default=str).encode("utf-8")
        print(f"🚀 Subiendo partición ({y}-{m}-{d}) -> {len(rows)} registros")
        s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=body, ContentType="application/json")
        print(f"   ✔️ OK -> s3://{S3_BUCKET}/{s3_key}")
        total_uploaded += 1
    print(f"\n📦 Total particiones subidas: {total_uploaded}")
    print("✅ STAGING S3 COMPLETADO\n")

# ==========================================================
# 3. RDS CREATION / CHECK WITH SECURITY GROUP OPEN
# ==========================================================
def ensure_open_security_group():
    ec2 = boto3.client(
        "ec2", aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        aws_session_token=AWS_SESSION_TOKEN, region_name=AWS_REGION
    )
    try:
        sgs = ec2.describe_security_groups(GroupNames=[SG_NAME])
        sg_id = sgs["SecurityGroups"][0]["GroupId"]
        print(f"🛡️ Security Group existente encontrado: {sg_id}")
        return sg_id
    except ec2.exceptions.ClientError as e:
        if "InvalidGroup.NotFound" in str(e):
            print(f"🛡️ Creando nuevo Security Group para RDS abierto al exterior...")
            vpcs = ec2.describe_vpcs(Filters=[{"Name": "isDefault", "Values": ["true"]}])
            vpc_id = vpcs["Vpcs"][0]["VpcId"] if vpcs["Vpcs"] else ec2.describe_vpcs()["Vpcs"][0]["VpcId"]
            sg = ec2.create_security_group(
                GroupName=SG_NAME, Description="Grupo para abrir el puerto de MySQL RDS al exterior", VpcId=vpc_id
            )
            sg_id = sg["GroupId"]
            ec2.authorize_security_group_ingress(
                GroupId=sg_id,
                IpPermissions=[{
                    "IpProtocol": "tcp", "FromPort": 3306, "ToPort": 3306,
                    "IpRanges": [{"CidrIp": "0.0.0.0/0", "Description": "MySQL public access"}]
                }]
            )
            print(f"🔓 Security Group {sg_id} configurado con regla de entrada abierta para el puerto 3306.")
            return sg_id
        raise e

def get_or_create_rds():
    print("\n" + "=" * 60)
    print("🗄️ ETAPA 3/4 - VERIFICACIÓN / CREACIÓN RDS")
    print("=" * 60)
    rds = boto3.client(
        "rds", aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        aws_session_token=AWS_SESSION_TOKEN, region_name=AWS_REGION
    )
    try:
        response = rds.describe_db_instances(DBInstanceIdentifier=RDS_IDENTIFIER)
        instance = response["DBInstances"][0]
        status = instance["DBInstanceStatus"]
        print(f"🔎 Estado RDS: {status}")
        if status != "available":
            print("⏳ Esperando a que RDS esté disponible...")
            rds.get_waiter("db_instance_available").wait(DBInstanceIdentifier=RDS_IDENTIFIER)
            instance = rds.describe_db_instances(DBInstanceIdentifier=RDS_IDENTIFIER)["DBInstances"][0]
        endpoint = instance["Endpoint"]["Address"]
        print(f"✅ RDS listo: {endpoint}")
        return endpoint
    except rds.exceptions.DBInstanceNotFoundFault:
        print("📦 RDS no existe → Asegurando grupo de seguridad...")
        sg_id = ensure_open_security_group()
        print("📦 Creando instancia de RDS vinculada al grupo abierto...")
        rds.create_db_instance(
            DBInstanceIdentifier=RDS_IDENTIFIER, DBInstanceClass="db.t3.micro",
            Engine="mysql", MasterUsername=os.getenv("MYSQL_USER"),
            MasterUserPassword=os.getenv("MYSQL_PASSWORD"), AllocatedStorage=20,
            PubliclyAccessible=True, BackupRetentionPeriod=0,
            VpcSecurityGroupIds=[sg_id]
        )
        print("⏳ Creando RDS (puede tardar varios minutos)...")
        rds.get_waiter("db_instance_available").wait(DBInstanceIdentifier=RDS_IDENTIFIER)
        instance = rds.describe_db_instances(DBInstanceIdentifier=RDS_IDENTIFIER)["DBInstances"][0]
        endpoint = instance["Endpoint"]["Address"]
        print(f"🎉 RDS creado: {endpoint}")
        return endpoint

# ==========================================================
# 4. LOAD EN RDS
# ==========================================================
def connect_rds(host):
    db_name = os.getenv("MYSQL_DB", "fraud_lakehouse")
    conn = pymysql.connect(
        host=host, user=os.getenv("MYSQL_USER"), password=os.getenv("MYSQL_PASSWORD"),
        port=3306, autocommit=True
    )
    with conn.cursor() as cursor:
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
    conn.select_db(db_name)
    return conn

def load_into_rds(records, host):
    print("\n" + "=" * 60)
    print("🚀 ETAPA 4/4 - CARGA EN RDS (MYSQL) - MAPEO ADAPTADO")
    print("=" * 60)
    conn = connect_rds(host)
    cursor = conn.cursor()
    print("🧱 Verificando tabla destino...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS fraud_features (
            transaction_id VARCHAR(255) PRIMARY KEY,
            user_id VARCHAR(255),
            amount DOUBLE,
            is_fraud INT,
            risk_score DOUBLE,
            event_time DATETIME,
            country VARCHAR(100),
            device_type VARCHAR(100),
            ingestion_time DATETIME
        )
    """)
    sql = """
        INSERT INTO fraud_features (
            transaction_id, user_id, amount, is_fraud,
            risk_score, event_time, country, device_type, ingestion_time
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
            amount=VALUES(amount),
            country=VALUES(country)
    """
    print(f"📥 Insertando {len(records)} registros reales adaptados...")
    batch = []
    for r in records:
        amount = float(r.get("amount", 0.0)) if r.get("amount") is not None else 0.0
        
        # Mapeamos 'is_fraud' y 'risk_score' basándonos en reglas lógicas de tu Silver si no vienen explícitos
        # Por ejemplo, si el estatus es 'declined' podríamos sospechar, si no, lo dejamos a 0
        is_fraud = 1 if r.get("status") == "declined" else 0
        risk_score = float(r.get("declined_ratio_1h", 0.0))
        
        # Formatear marcas de tiempo
        ev_time = r.get("event_time")
        event_time_str = str(ev_time).replace("T", " ").split(".")[0] if ev_time else datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        ing_time = r.get("ingestion_time") or r.get("ingested_at") or datetime.now()
        ingestion_time_str = str(ing_time).replace("T", " ").split(".")[0]
        
        # REGLAS DE MAPEO CORREGIDAS SEGÚN TU JSON:
        batch.append((
            r.get("payment_id", f"gen-{uuid4().hex[:8]}"),    # 🌟 Tu 'transaction_id' real es 'payment_id'
            r.get("customer_id", "unknown"),                 # 🌟 Tu 'user_id' real es 'customer_id'
            amount,
            is_fraud,
            risk_score,
            event_time_str,
            r.get("country", "unknown"),                      # 🌟 'ES' se mapeará correctamente
            r.get("device_id", "unknown"),                    # 🌟 Mapeamos 'device_id' en la columna de dispositivo
            ingestion_time_str
        ))
    cursor.executemany(sql, batch)
    print("✅ Inserción completa en RDS con datos mapeados correctamente.")
    conn.close()
    print("🎯 PIPELINE COMPLETADO CON ÉXITO")
    
# ==========================================================
# MAIN PIPELINE
# ==========================================================
if __name__ == "__main__":
    print("\n🚀 INICIANDO PIPELINE SILVER → S3 → RDS\n")
    silver = fetch_silver()
    if not silver:
        print("⚠️ Pipeline abortado: sin datos en Silver")
        exit()
    upload_silver_to_s3(silver)
    endpoint = get_or_create_rds()
    load_into_rds(silver, endpoint)