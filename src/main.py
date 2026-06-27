# src/main.py
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, FloatType, LongType,
    BooleanType, TimestampType,
)

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

schema_pagamentos = StructType([
    StructField("id_pedido",          StringType(),    False),
    StructField("forma_pagamento",    StringType(),    True),
    StructField("valor_pagamento",    FloatType(),     True),
    StructField("status",             BooleanType(),   True),
    StructField("data_processamento", TimestampType(), True),
    StructField("avaliacao_fraude", StructType([
        StructField("fraude", BooleanType(), True),
        StructField("score",  FloatType(),   True),
    ]), True),
])

schema_pedidos = StructType([
    StructField("id_pedido",      StringType(),    False),
    StructField("produto",        StringType(),    True),
    StructField("valor_unitario", FloatType(),     True),
    StructField("quantidade",     LongType(),      True),
    StructField("data_criacao",   TimestampType(), True),
    StructField("uf",             StringType(),    True),
    StructField("id_cliente",     LongType(),      True),
])

print("Abrindo a sessao spark")
spark = SparkSession.builder.appName("Analise de Fraude").getOrCreate()

print("Abrindo o dataframe de pagamentos...")
pagamentos_path = os.path.join(PROJECT_ROOT, "dataset", "input", "pagamentos", "*.json.gz")
pagamentos = (
    spark.read
    .schema(schema_pagamentos)
    .option("compression", "gzip")
    .json(pagamentos_path)
)

pagamentos.printSchema()
pagamentos.show(5, truncate=False)

print("Abrindo o dataframe de pedidos...")
pedidos_path = os.path.join(PROJECT_ROOT, "dataset", "input", "pedidos", "*.csv.gz")
pedidos = (
    spark.read
    .schema(schema_pedidos)
    .option("compression", "gzip")
    .option("header", "true")
    .option("sep", ";")
    .csv(pedidos_path)
)

pedidos.printSchema()
pedidos.show(5, truncate=False)

spark.stop()
