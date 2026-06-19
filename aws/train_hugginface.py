import os
import boto3
import time
import pandas as pd
import numpy as np
from io import StringIO
from datetime import datetime
from dotenv import load_dotenv
from datasets import Dataset
from transformers import AutoTokenizer, AutoModelForSequenceClassification, TrainingArguments, Trainer
import evaluate

load_dotenv()

# ==========================================================
# CONFIGURACIÓN
# ==========================================================
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET = os.getenv("S3_BUCKET", "lakehouse-ricardo-sdb-2026")

# Nombre del modelo base en Hugging Face y directorios de salida
MODEL_NAME = "distilbert-base-uncased"
OUTPUT_DIR = "./fraud_model_results"
LOCAL_DATASET_DIR = "./datasets"

# ==========================================================
# 📥 PASO 1: DESCARGAR DATASET DESDE S3
# ==========================================================
def download_analytics_data():
    print("☁️ Descargando reporte consolidado de S3 para Machine Learning...")
    s3 = boto3.client(
        "s3", aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY,
        aws_session_token=AWS_SESSION_TOKEN, region_name=AWS_REGION
    )
    response = s3.get_object(Bucket=S3_BUCKET, Key="analytics/fraud_insights_report.csv")
    csv_data = response["Body"].read().decode("utf-8")
    df = pd.read_csv(StringIO(csv_data))
    print(f"   ✔️ Dataset importado con éxito: {len(df)} registros.")
    return df

# ==========================================================
# 🧼 PASO 2: PREPARAR Y GUARDAR LOS DATOS (NLP FORMAT)
# ==========================================================
def prepare_and_save_dataset(df):
    print("🧼 Transformando variables tabulares en prompts textuales para NLP...")
    
    # Creamos la cadena de texto estructurada para el modelo Transformer
    df["text"] = df.apply(lambda r: (
        f"Transaction from country {r.get('country', 'UNKNOWN')} with amount {r.get('amount', 0.0)} EUR. "
        f"Status: {r.get('status', 'unknown')}. Risk score evaluation: {r.get('puntuacion_riesgo', 0)}."
    ), axis=1)
    
    # Generamos la etiqueta binaria: 1 si es rechazado o el riesgo es crítico (>10), 0 si no
    df["label"] = df.apply(lambda r: 1 if (r.get("status") == "declined" or r.get("puntuacion_riesgo", 0) > 10) else 0, axis=1)
    
    df_clean = df[["text", "label"]].dropna()
    
    # 💾 RESPALDO LOCAL: Guardar el dataset procesado en formato CSV local
    if not os.path.exists(LOCAL_DATASET_DIR):
        os.makedirs(LOCAL_DATASET_DIR)
        print(f"   📁 Carpeta '{LOCAL_DATASET_DIR}' creada.")
        
    local_csv_path = os.path.join(LOCAL_DATASET_DIR, "fraud_processed_nlp.csv")
    df_clean.to_csv(local_csv_path, index=False, encoding="utf-8")
    print(f"   💾 Dataset guardado localmente en: {local_csv_path}")
    
    # Dividir de forma clásica en entrenamiento (80%) y validación (20%)
    df_train = df_clean.sample(frac=0.8, random_state=42)
    df_val = df_clean.drop(df_train.index)
    
    # Convertir a objetos Dataset nativos de Hugging Face
    train_dataset = Dataset.from_pandas(df_train.reset_index(drop=True))
    val_dataset = Dataset.from_pandas(df_val.reset_index(drop=True))
    
    return train_dataset, val_dataset

# ==========================================================
# 📊 MÉTRICAS DE EVALUACIÓN
# ==========================================================
metric = evaluate.load("accuracy")
def compute_metrics(eval_pred):
    logits, labels = eval_pred
    predictions = np.argmax(logits, axis=-1)
    return metric.compute(predictions=predictions, references=labels)

# ==========================================================
# 🚀 EJECUCIÓN DEL FINE-TUNING
# ==========================================================
if __name__ == "__main__":
    df_raw = download_analytics_data()
    train_data, val_data = prepare_and_save_dataset(df_raw)
    
    print(f"🧠 Inicializando tokenizador y modelo base: {MODEL_NAME}...")
    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
    model = AutoModelForSequenceClassification.from_pretrained(MODEL_NAME, num_labels=2)
    
    # Tokenizar las cadenas de texto construidas
    def tokenize_function(examples):
        return tokenizer(examples["text"], padding="max_length", truncation=True, max_length=128)
    
    print("🪙 Tokenizando datasets de entrenamiento y validación...")
    tokenized_train = train_data.map(tokenize_function, batched=True)
    tokenized_val = val_data.map(tokenize_function, batched=True)
    
    print("⚙️ Configurando hiperparámetros de entrenamiento...")
    training_args = TrainingArguments(
        output_dir=OUTPUT_DIR,
        eval_strategy="epoch",
        save_strategy="epoch",
        learning_rate=2e-5,
        per_device_train_batch_size=8,
        per_device_eval_batch_size=8,
        num_train_epochs=1,
        weight_decay=0.01,
        logging_steps=10,
        load_best_model_at_end=True
    )
    
    print("🏋️ Iniciando Fine-Tuning del modelo (Trainer)...")
    trainer = Trainer(
        model=model,
        args=training_args,
        train_dataset=tokenized_train,
        eval_dataset=tokenized_val,
        compute_metrics=compute_metrics,
    )
    
    trainer.train()
    
    print("\n📊 Evaluando rendimiento final sobre el conjunto de validación...")
    eval_results = trainer.evaluate()
    print(f"   ✔️ Resultados obtenidos: {eval_results}")
    
    # Guardar localmente el modelo final refinado
    final_model_path = f"{OUTPUT_DIR}_final"
    print(f"💾 Guardando pesos del modelo final en: {final_model_path}")
    model.save_pretrained(final_model_path)
    tokenizer.save_pretrained(final_model_path)
    
    print("\n🎉 === PROCESO DE MACHINE LEARNING COMPLETADO CON ÉXITO ===")