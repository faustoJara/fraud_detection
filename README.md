# fraud_detection
Sistema de detección de pagos fraudulentos en arquitectura Lakehouse


🧩 PARTE 1 — Generación de eventos en Kafka
Debes desarrollar un generador de eventos de pagos que publique en un topic de Kafka.
Cada evento deberá incluir al menos los siguientes campos:
event_time
payment_id
customer_id
card_id
merchant_id
device_id
ip
country
amount
currency
status (approved / declined)
mcc (merchant category code)


El generador deberá simular comportamiento realista:
Pagos normales
Reintentos
Casos sospechosos (alta frecuencia, múltiples comercios, etc.)



🥉 PARTE 2 — Bronze (datos crudos)
Implementa un proceso en Spark Streaming que:
Lea desde Kafka.
Guarde los datos en Iceberg en la capa bronze.
No realice transformaciones complejas.
Mantenga el esquema original.


La tabla bronze debe reflejar los datos tal y como llegan.

🥈 PARTE 3 — Silver (limpieza y enriquecimiento)
Implementa un job en Spark que:
Lea desde bronze.
Convierta correctamente los tipos de datos.
Elimine duplicados.
Añada variables derivadas mediante ventanas temporales.


Algunas ideas de enriquecimiento:
Número de transacciones por tarjeta en los últimos 5 minutos.
Número de comercios distintos en 10 minutos.
Número de países distintos en 1 hora.
Número de tarjetas distintas por dispositivo.
Ratio de transacciones rechazadas.


El resultado debe almacenarse en la capa silver.

🥇 PARTE 4 — Gold (detección de fraude)
Implementa un proceso que:
Lea desde silver.
Defina reglas de detección de fraude.
Genere una tabla fraud_alerts con:
payment_id
variables principales
risk_score
lista de motivos (reasons)


Genere adicionalmente una tabla preparada para análisis de relaciones (modelo tabular).


Ambas tablas deben almacenarse en gold.

📊 PARTE 5 — Consulta y visualización
Conecta Trino a Iceberg.
Verifica que puedes consultar las tablas silver y gold.
Conecta Superset a Trino.
Crea un dashboard que muestre:
Número total de transacciones
Número de alertas de fraude
Evolución temporal
Comercios o tarjetas con mayor riesgo



🔁 PARTE 6 — Orquestación bajo demanda con Airflow
Crea un DAG en Airflow que, al ejecutarse manualmente:
Compacte la tabla Iceberg para reducir small files.
Genere el dataset necesario para análisis de grafo.
Exporte los datos necesarios para Neo4j.
Lance el proceso de carga en Neo4j.


El DAG deberá aceptar parámetros para:
Tabla origen
Rango temporal
Nombre del grafo



🕸 PARTE 7 — Modelo de grafo en Neo4j
Diseña un modelo de grafo que represente:
Clientes
Tarjetas
Dispositivos
Comercios
Pagos


Debes:
Crear nodos.
Crear relaciones entre entidades.
Ejecutar consultas Cypher que permitan detectar:
Dispositivos compartidos por múltiples tarjetas.
Tarjetas utilizadas en múltiples países.
Comercios conectados a múltiples entidades sospechosas.
Posibles agrupaciones de comportamiento anómalo.
☁️ PARTE 8 — Script de exportación a S3 con Python + boto3
Crear un script que:
Lea datos desde Gold 


Genere ficheros Parquet o JSON


Suba los datos a S3


Organice por particiones temporales
   	    gold/
        		year=2026/month=03/day=18/
            		fraud_alerts.json



☁️ PARTE 9 — Script: S3 (Silver) → RDS MySQL
Construir un script que:
Lea datos de Silver almacenados en Amazon S3


Procese los datos (features ya calculadas)


Inserte los datos en una base de datos MySQL en RDS
☁️ PARTE 10 — ELT Job o Script de unión S3 + RDS
Leer datos de S3 (Silver o Gold)
Leer datos de RDS MySQL
Limpiar los datos:


Campo
Tratamiento
payment_id
Quitar espacios, convertir a string
card_id
Quitar espacios, unificar mayúsculas/minúsculas
amount
Convertir a float, valores negativos o nulos → eliminar o corregir
event_time
Convertir a timestamp, corregir formato ISO
country
Unificar códigos de país (ES, es, España → ES)
status
Normalizar valores (approved / declined)

Eliminación de duplicados por payment_id
Manejo de valores nulos
risk_score: si no está calculado, asignar 0 o recalcular


tx_5m, merchants_10m, countries_1h: rellenar con 0 si nulo


amount, card_id: eliminar registros críticos si faltan
Unirlos en un solo dataset mediante payment_id
Aplicar filtros relevantes
 Filtrar por riesgo alto
 Filtrar por frecuencia transaccional
 Filtrar por geografía sospechosa
Guardar resultados filtrados en S3 o Athena para análisis
📝 PARTE 11: Consultas y análisis con Athena
Usar los datasets filtrados y consolidados en S3 de la Parte 10 para crear consultas en Athena que permitan:
Detectar transacciones de alto riesgo


Identificar tarjetas con actividad sospechosa


Analizar patrones geográficos de fraude
Los resultados se usarán para dashboards, análisis de fraude y posibles alertas operativas por lo tanto deberás guardar los resultados en S3 (CSV o Parquet) para su posterior análisis.
📝 Parte 12 — Fine-tuning con Hugging Face
Descargar los datasets filtrados o consolidados desde Athena/S3


Preparar los datos para entrenamiento


Hacer fine-tuning de un modelo pre-entrenado en Hugging Face


Evaluar rendimiento y guardar el modelo en Hugging Face Hub





