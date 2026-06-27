# src/main.py
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

print("Abrindo a sessao spark")
spark = SparkSession.builder.appName("Analise de Fraude").getOrCreate()

print("Abrindo o dataframe de pagamentos, deixando o Spark inferir o schema")
pagamentos = spark.read.option("compression", "gzip").json("dataset\\input\\pagamentos\\*.json.gz")

pagamentos.printSchema()
pagamentos.show(5, truncate=False)

# print("Abrindo o dataframe de pedidos, deixando o Spark inferir o schema")
# pedidos = spark.read.option("compression", "gzip") \
#                     .option("header", "true") \
#                     .option("inferSchema", "true") \
#                     .option("sep", ";") \
#                     .csv("./dataset/data/input/datasets-csv-pedidos/data/pedidos/")

# pedidos.printSchema()

# print("Adicionando a coluna valor_total")
# pedidos = pedidos.withColumn("valor_total", F.col("valor_unitario") * F.col("quantidade"))
# pedidos.show(5, truncate=False)

# print("executando a logica de negocio para obter os top 10 clientes em valor total de pedidos")
# calculado = pedidos.groupBy("id_cliente") \
#     .agg(F.sum("valor_total").alias("valor_total")) \
#     .orderBy(F.desc("valor_total")) \
#     .limit(10)

# print("criando o dataframe final incluindo os dados do cliente")
# pedidos_clientes = calculado.join(clientes, clientes.id == calculado.id_cliente, "inner") \
#     .select(calculado.id_cliente, clientes.nome, clientes.email, calculado.valor_total)

# pedidos_clientes.show(20, truncate=False)

# pedidos_clientes.write.mode("overwrite").parquet("./data-engineering-pyspark/data/output/pedidos_por_cliente")

spark.stop()