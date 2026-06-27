# src/main.py
from config.settings import carregar_config
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, FloatType, LongType,
    BooleanType, TimestampType,
)

config = carregar_config()

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

app_name          = config["spark"]["app_name"]
pagamentos_path   = config["paths"]["pagamentos"]
pedidos_path      = config["paths"]["pedidos"]
output_path       = config["paths"]["output"]
compression_json  = config["file_options"]["pagamentos_json"]["compression"]
compression_csv   = config["file_options"]["pedidos_csv"]["compression"]
header_pedidos    = config["file_options"]["pedidos_csv"]["header"]
separator_pedidos = config["file_options"]["pedidos_csv"]["sep"]

print("Abrindo a sessao spark")
spark = SparkSession.builder.appName(app_name).getOrCreate()

print("Abrindo o dataframe de pagamentos...")
pagamentos = (
    spark.read
    .schema(schema_pagamentos)
    .option("compression", compression_json)
    .json(pagamentos_path)
)

pagamentos.printSchema()
pagamentos.show(5, truncate=False)

print("Abrindo o dataframe de pedidos...")
pedidos = (
    spark.read
    .schema(schema_pedidos)
    .option("compression", compression_csv)
    .option("header", header_pedidos)
    .option("sep", separator_pedidos)
    .csv(pedidos_path)
)

pedidos.printSchema()
pedidos.show(5, truncate=False)

spark.stop()
