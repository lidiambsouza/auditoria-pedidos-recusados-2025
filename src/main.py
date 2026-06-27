# src/main.py
from config.settings import carregar_config
from session.spark_session import SparkSessionManager
from io_utils.data_handler import DataHandler
from processing.transformations import Transformation

def main():
    config = carregar_config()

    app_name          = config["spark"]["app_name"]
    pagamentos_path   = config["paths"]["pagamentos"]
    pedidos_path      = config["paths"]["pedidos"]
    ralatorio_final_path       = config["paths"]["output"]
    compression_json_pagamentos  = config["file_options"]["pagamentos_json"]["compression"]
    compression_csv_pedidos   = config["file_options"]["pedidos_csv"]["compression"]
    header_pedidos    = config["file_options"]["pedidos_csv"]["header"]
    separator_pedidos = config["file_options"]["pedidos_csv"]["sep"]
    print(f"Obtido o app name: {app_name}")



    print("Abrindo a sessao spark")
    spark = SparkSessionManager.get_spark_session(app_name=app_name)

    data_handler = DataHandler(spark)
    transformer = Transformation()
    
    print("Abrindo o dataframe de pagamentos...")
    print(f"""
        Obtidos os seguintes parâmetros de pagamentos: 
        - path: {pagamentos_path}
        - compression: {compression_json_pagamentos}
        """)
    pagamentos_df = data_handler.load_pagamentos(path = pagamentos_path, compression = compression_json_pagamentos)


    pagamentos_df.printSchema()
    pagamentos_df.show(5, truncate=False)

    print("Abrindo o dataframe de pedidos...")
    print(f"""
        Obtidos os seguintes parâmetros de pedidos: 
        - path: {pedidos_path}
        - compression: {compression_csv_pedidos}
        - header: {header_pedidos}
        - separator: {separator_pedidos}
        """)
    pedidos_df = data_handler.load_pedidos(path = pedidos_path, compression=compression_csv_pedidos, header=header_pedidos, sep=separator_pedidos)


    pedidos_df.printSchema()
    pedidos_df.show(5, truncate=False)

    ### criar relatorio###
    print("Filtrando pedidos de 2025")
    pedidos_2025_df = transformer.get_pedidos_2025(pedidos_df) #diminuir a base

    print("Adicionando a coluna com o valor total do pedido")
    pedidos_com_valor_total_df = transformer.add_valor_total_pedidos(pedidos_2025_df)#adiciona  a informação

    print("Filtrando pagamentos recusados e não fraudulentos")
    pagamentos_recusados_legitimos_df = transformer.get_pagamentos_recusados_legitimos(pagamentos_df)

    print("Unificando pedidos e pagamentos")
    pagamentos_pedidos_df = transformer.join_pedido_pagamento(pedidos_com_valor_total_df, pagamentos_recusados_legitimos_df)
    pagamentos_pedidos_df.show(10, truncate=False)

    print("Criando o relatorio de pedidos recusados e não fraudulentos para o parquet")
    relatorio_pedidos_recusados_sem_fraudes_df = transformer.relatorio_pedido_pagamento(pagamentos_pedidos_df)

    relatorio_pedidos_recusados_sem_fraudes_df.show(10, truncate=False)

    print(f"Obtido o path de saída: {ralatorio_final_path}")
    data_handler.write_parquet(relatorio_pedidos_recusados_sem_fraudes_df, ralatorio_final_path, validate_schema=True)

    spark.stop()

if __name__ == "__main__":
  main()