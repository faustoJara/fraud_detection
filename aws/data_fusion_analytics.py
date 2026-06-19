import os
import json
import boto3
import pymysql
import pandas as pd
import numpy as np
from io import StringIO
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# ==========================================================
# CONFIGURACIÓN
# ==========================================================
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET = os.getenv("S3_BUCKET")
RDS_IDENTIFIER = os.getenv("RDS_IDENTIFIER", "fraud-mysql-db-parte9")
MYSQL_DB = os.getenv("MYSQL_DB", "fraud_lakehouse")

# Inicializar cliente S3
s3_client = boto3.client(
    "s3", aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    aws_session_token=AWS_SESSION_TOKEN, region_name=AWS_REGION
)

# ==========================================================
# 🚀 PASO 1 Y 2: LEER DATOS (S3 GOLD Y RDS MYSQL)
# ==========================================================
def load_gold_from_s3():
    print("☁️ Leyendo datos de la capa GOLD en S3...")
    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=S3_BUCKET, Prefix="gold/")
    
    all_records = []
    for page in pages:
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".json") or obj["Key"].endswith(".csv"):
                response = s3_client.get_object(Bucket=S3_BUCKET, Key=obj["Key"])
                
                if obj["Key"].endswith(".json"):
                    data = json.loads(response["Body"].read().decode("utf-8"))
                    if isinstance(data, list):
                        all_records.extend(data)
                    else:
                        all_records.append(data)
                
                elif obj["Key"].endswith(".csv"):
                    csv_data = response["Body"].read().decode("utf-8")
                    df_temp = pd.read_csv(StringIO(csv_data))
                    all_records.extend(df_temp.to_dict(orient="records"))
                    
    print(f"   ✔️ Registros totales recuperados de s3://{S3_BUCKET}/gold/: {len(all_records)}")
    return pd.DataFrame(all_records)

def load_data_from_rds():
    print("🗄️ Conectando a AWS RDS MySQL...")
    rds_client = boto3.client(
        "rds", aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        aws_session_token=AWS_SESSION_TOKEN, region_name=AWS_REGION
    )
    ins = rds_client.describe_db_instances(DBInstanceIdentifier=RDS_IDENTIFIER)["DBInstances"][0]
    host = ins["Endpoint"]["Address"]
    
    conn = pymysql.connect(
        host=host, user=os.getenv("MYSQL_USER"), password=os.getenv("MYSQL_PASSWORD"),
        database=MYSQL_DB, port=3306
    )
    
    query = "SELECT * FROM fraud_features;"
    df_rds = pd.read_sql(query, conn)
    conn.close()
    print(f"   ✔️ Registros leídos de RDS MySQL: {len(df_rds)}")
    return df_rds

# ==========================================================
# 🛠️ PASO 3: LIMPIAR LOS DATOS (REGLAS TRATAMIENTO)
# ==========================================================
def clean_and_normalize(df_s3, df_rds):
    print("\n🧼 Iniciando proceso de limpieza y unificación...")
    
    # Unificamos nombres de columnas de RDS para alinearlos con el esquema real de S3 Gold
    if "transaction_id" in df_rds.columns:
        df_rds.rename(columns={"transaction_id": "payment_id"}, inplace=True)
    if "user_id" in df_rds.columns:
        df_rds.rename(columns={"user_id": "customer_id"}, inplace=True)
        
    # --- Reglas de Tratamiento ---
    # payment_id: Quitar espacios, convertir a string
    for df in [df_s3, df_rds]:
        if "payment_id" in df.columns:
            df["payment_id"] = df["payment_id"].astype(str).str.strip()
            
    # card_id: Quitar espacios, unificar a mayúsculas
    for df in [df_s3, df_rds]:
        if "card_id" in df.columns:
            df["card_id"] = df["card_id"].astype(str).str.strip().str.upper()
            
    # amount: Convertir a float
    for df in [df_s3, df_rds]:
        if "amount" in df.columns:
            df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
            
    # event_time: Convertir a timestamp limpio
    for df in [df_s3, df_rds]:
        if "event_time" in df.columns:
            df["event_time"] = pd.to_datetime(df["event_time"], errors="coerce")
            
    # country: Unificar códigos (ES, es, España -> ES)
    for df in [df_s3, df_rds]:
        if "country" in df.columns:
            df["country"] = df["country"].astype(str).str.strip().str.upper()
            df["country"] = df["country"].replace({"ESPAÑA": "ES", "SPAIN": "ES", "ES": "ES", "UNKNOWN": np.nan})
            
    # status: Normalizar valores (approved / declined)
    for df in [df_s3, df_rds]:
        if "status" in df.columns:
            df["status"] = df["status"].astype(str).str.strip().str.lower()
            
    # Aseguramos que la puntuación de riesgo nativa de S3 sea numérica antes del merge
    if "puntuacion_riesgo" in df_s3.columns:
        df_s3["puntuacion_riesgo"] = pd.to_numeric(df_s3["puntuacion_riesgo"], errors="coerce")
            
    # Eliminación de duplicados por payment_id
    df_s3.drop_duplicates(subset=["payment_id"], keep="first", inplace=True)
    df_rds.drop_duplicates(subset=["payment_id"], keep="first", inplace=True)
    
    # Eliminación de registros críticos si falta amount o card_id en el origen de S3
    df_s3.dropna(subset=["amount", "card_id"], inplace=True)
    df_s3 = df_s3[df_s3["amount"] > 0]
    
    print("✅ Normalización y depuración completada.")
    return df_s3, df_rds

# ==========================================================
# 🔗 PASO 4 Y 5: UNIÓN (MERGE) Y FILTRADO AVANZADO
# ==========================================================
def fuse_and_filter(df_s3, df_rds):
    print("\n🔗 Uniendo datasets mediante payment_id (Inner Join)...")
    
    # Extraemos de RDS únicamente las dimensiones operacionales complementarias (como device_type)
    # Ignoramos la columna 'risk_score' de RDS para gobernar el proceso con el riesgo real de tu Gold
    columnas_rds = ["payment_id", "device_type"] if "device_type" in df_rds.columns else ["payment_id"]
    df_fusion = pd.merge(df_s3, df_rds[columnas_rds], on="payment_id", how="inner")
    print(f"   📊 Dataset unificado final: {len(df_fusion)} filas.")
    
    # Control seguro de nulidad sobre la puntuación de riesgo nativa
    if "puntuacion_riesgo" in df_fusion.columns:
        df_fusion["risk_score"] = df_fusion["puntuacion_riesgo"].fillna(0.0)
    else:
        df_fusion["risk_score"] = 0.0
            
    # Rellenar con 0 métricas de ventanas de tiempo si vienen nulas
    metricas_ventanas = ["tx_by_card_5m", "distinct_merchants_10m", "distinct_countries_1h", "declined_ratio_1h"]
    for col in metricas_ventanas:
        if col in df_fusion.columns:
            df_fusion[col] = df_fusion[col].fillna(0)
            
    print("\n🎯 Aplicando filtros de seguridad analítica con Riesgo Real (S3 Gold)...")
    # Filtros de negocio adaptados a tu escala de riesgo entera intacta
    f_riesgo = (df_fusion["risk_score"] > 10) | (df_fusion["status"] == "declined")
    f_frecuencia = df_fusion["tx_by_card_5m"] > 1
    f_geografia = (df_fusion["country"] != "ES") | (df_fusion["country"].isna())
    
    # Dataset final filtrado de sospechas
    df_alertas = df_fusion[f_riesgo | f_frecuencia | f_geografia].copy()
    print(f"   ⚠️ Registros sospechosos aislados para auditoría: {len(df_alertas)}")
    return df_alertas

# ==========================================================
# 💾 PASO 6: GUARDAR EN S3 ANALYTICS (CSV)
# ==========================================================
def save_results_to_s3(df_final):
    print("\n💾 Guardando resultados enriquecidos en S3 (Capa Analytics)...")
    csv_buffer = StringIO()
    df_final.to_csv(csv_buffer, index=False)
    
    destination_key = "analytics/fraud_insights_report.csv"
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=destination_key,
        Body=csv_buffer.getvalue(),
        ContentType="text/csv"
    )
    print(f"🎉 ¡ÉXITO! Reporte consolidado disponible en: s3://{S3_BUCKET}/{destination_key}")

# ==========================================================
# EJECUCIÓN PRINCIPAL
# ==========================================================
if __name__ == "__main__":
    print("============================================================")
    print(" INICIANDO PROCESO DE INTEGRACION DE CAPAS ANALITICAS")
    print("============================================================")
    
    df_s3 = load_gold_from_s3()
    df_rds = load_data_from_rds()
    
    if df_s3.empty or df_rds.empty:
        print("❌ Error: Uno de los orígenes está vacío. Deteniendo.")
        exit()
        
    df_s3, df_rds = clean_and_normalize(df_s3, df_rds)
    df_alertas = fuse_and_filter(df_s3, df_rds)
    
    if not df_alertas.empty:
        save_results_to_s3(df_alertas)
    else:
        print("🎰 No se encontraron registros de sospecha alta según las reglas.")
    print("\n🏁 PROCESO FINALIZADO CON ÉXITO")