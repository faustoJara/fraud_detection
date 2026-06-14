import os
import sys
print(f"Python ejecutable: {sys.executable}")
print(f"JAVA_HOME actual: {os.environ.get('JAVA_HOME')}")
print(f"HADOOP_HOME actual: {os.environ.get('HADOOP_HOME')}")

from pyspark.sql import SparkSession
print("Intentando crear SparkSession...")
spark = SparkSession.builder.appName("Test").getOrCreate()
print("¡SparkSession creada con éxito!")