# src/pipeline/pipeline.py
from pyspark.sql import SparkSession
from io_utils.data_handler import DataHandler
from processing.transformations import Transformation

class Pipeline:
    """
    Encapsula a lógica de execução do pipeline de dados.
    """
    def __init__(self, spark: SparkSession, transformer: Transformation):
        self.data_handler = DataHandler(spark)
        self.transformer = transformer

    def run(self, config):
        """
        Executa o pipeline completo: carga, transformação, e salvamento.
        """
        print("Pipeline iniciado...")    

        pagamentos_path   = config["paths"]["pagamentos"]
        pedidos_path      = config["paths"]["pedidos"]
        ralatorio_final_path       = config["paths"]["output"]
        compression_json_pagamentos  = config["file_options"]["pagamentos_json"]["compression"]
        compression_csv_pedidos   = config["file_options"]["pedidos_csv"]["compression"]
        header_pedidos    = config["file_options"]["pedidos_csv"]["header"]
        separator_pedidos = config["file_options"]["pedidos_csv"]["sep"]    
        
        print("Abrindo o dataframe de pagamentos...")
        print(f"""
        Obtidos os seguintes parâmetros de pagamentos: 
        - path: {pagamentos_path}
        - compression: {compression_json_pagamentos}
        """)
        pagamentos_df = self.data_handler.load_pagamentos(path = pagamentos_path, compression = compression_json_pagamentos)


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
        pedidos_df = self.data_handler.load_pedidos(path = pedidos_path, compression=compression_csv_pedidos, header=header_pedidos, sep=separator_pedidos)


        pedidos_df.printSchema()
        pedidos_df.show(5, truncate=False)

        ### criar relatorio###
        print("Filtrando pedidos de 2025")
        pedidos_2025_df = self.transformer.get_pedidos_2025(pedidos_df) #diminuir a base

        print("Adicionando a coluna com o valor total do pedido")
        pedidos_com_valor_total_df = self.transformer.add_valor_total_pedidos(pedidos_2025_df)#adiciona  a informação

        print("Filtrando pagamentos recusados e não fraudulentos")
        pagamentos_recusados_legitimos_df = self.transformer.get_pagamentos_recusados_legitimos(pagamentos_df)

        print("Unificando pedidos e pagamentos")
        pagamentos_pedidos_df = self.transformer.join_pedido_pagamento(pedidos_com_valor_total_df, pagamentos_recusados_legitimos_df)
        pagamentos_pedidos_df.show(10, truncate=False)

        print("Criando o relatorio de pedidos recusados e não fraudulentos para o parquet")
        relatorio_pedidos_recusados_sem_fraudes_df = self.transformer.relatorio_pedido_pagamento(pagamentos_pedidos_df)

        relatorio_pedidos_recusados_sem_fraudes_df.show(10, truncate=False)

        print(f"Obtido o path de saída: {ralatorio_final_path}")
        self.data_handler.write_parquet(relatorio_pedidos_recusados_sem_fraudes_df, ralatorio_final_path, validate_schema=True)

        print("Pipeline concluído com sucesso!")
      