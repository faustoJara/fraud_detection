@echo off
echo ===================================================
echo Limpiando entorno y configurando Lakehouse...
echo ===================================================

echo 0. Limpiando contenedores Docker activos...
docker-compose down -v
docker system prune -f

echo 1. Creando entorno Conda (lakehouse_env)...
call conda create -n lakehouse_env python=3.10 -y
call conda activate lakehouse_env

echo 2. Instalando JDK 17 (Necesario para Spark 3.5.0)...
call conda install -c conda-forge openjdk=17 -y

echo 3. Instalando dependencias desde requirements.txt...
pip install -r requirements.txt

echo 4. Estructura de directorios...
mkdir dags part1_kafka_producer part2_3_4_spark_jobs part7_neo4j part8_9_10_scripts part12_ml_huggingface data\minio data\mysql

echo 5. Levantando contenedores Docker...
docker-compose up -d

echo ===================================================
echo Entorno listo. Spark 3.5.0 + Java 17 configurados.
echo ===================================================
pause