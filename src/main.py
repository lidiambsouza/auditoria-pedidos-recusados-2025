# src/main.py
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

print("Abrindo a sessao spark")
spark = SparkSession.builder.appName("Analise de Fraude").getOrCreate()

print("Abrindo o dataframe de pagamentos, deixando o Spark inferir o schema")
pagamentos_path = os.path.join(BASE_DIR, "dataset", "input", "pagamentos", "*.json.gz")
pagamentos = spark.read.option("compression", "gzip").json(pagamentos_path)

pagamentos.printSchema()
pagamentos.show(5, truncate=False)

print("Abrindo o dataframe de pedidos, deixando o Spark inferir o schema")
pedidos_path = os.path.join(BASE_DIR, "dataset", "input", "pedidos", "*.csv.gz")
pedidos = spark.read.option("compression", "gzip") \
                    .option("header", "true") \
                    .option("inferSchema", "true") \
                    .option("sep", ";") \
                    .csv(pedidos_path)

pedidos.printSchema()



spark.stop()