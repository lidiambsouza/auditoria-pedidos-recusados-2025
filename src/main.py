# src/main.py
from config.settings import carregar_config
from session.spark_session import SparkSessionManager
from io_utils.data_handler import DataHandler
from processing.transformations import Transformation

config = carregar_config()
transformer = Transformation()

app_name          = config["spark"]["app_name"]
pagamentos_path   = config["paths"]["pagamentos"]
pedidos_path      = config["paths"]["pedidos"]
output_path       = config["paths"]["output"]
compression_json  = config["file_options"]["pagamentos_json"]["compression"]
compression_csv   = config["file_options"]["pedidos_csv"]["compression"]
header_pedidos    = config["file_options"]["pedidos_csv"]["header"]
separator_pedidos = config["file_options"]["pedidos_csv"]["sep"]

print("Abrindo a sessao spark")
spark = SparkSessionManager.get_spark_session(app_name=app_name)
dh = DataHandler(spark)

print("Abrindo o dataframe de pagamentos...")
pagamentos = dh.load_pagamentos(path = pagamentos_path, compression = compression_json)


pagamentos.printSchema()
pagamentos.show(5, truncate=False)

print("Abrindo o dataframe de pedidos...")
pedidos = dh.load_pedidos(path = pedidos_path, compression=compression_csv, header=header_pedidos, sep=separator_pedidos)


pedidos.printSchema()
pedidos.show(5, truncate=False)

### criar relatorio###
pedidos_2025 = transformer.get_pedidos_2025(pedidos) #diminuir a base
pedidos_com_total = transformer.add_valor_total_pedidos(pedidos_2025)#adiciona  a informação

pagamentos_recusados_legitimos = transformer.get_pagamentos_recusados_legitimos(pagamentos)

pagamentos_pedidos = transformer.join_pedido_pagamento(pedidos_com_total, pagamentos_recusados_legitimos)
pagamentos_pedidos.show(10, truncate=False)

relatorio = transformer.relatorio_pedido_pagamento(pagamentos_pedidos)

relatorio.show(10, truncate=False)
spark.stop()
