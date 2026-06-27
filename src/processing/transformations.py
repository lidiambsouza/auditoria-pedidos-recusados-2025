# src/processing/transformations.py
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType


class Transformation:
    """Contém as transformações e regras de negócio da aplicação."""

    def get_pedidos_2025(self, pedidos_df: DataFrame) -> DataFrame:
        """Retorna apenas os pedidos do ano de 2025."""
        return pedidos_df.filter(F.year(F.col("data_criacao_pedido")) == 2025)

    def add_valor_total_pedidos(self, pedidos_df: DataFrame) -> DataFrame:
        """Adiciona a coluna valor_total_pedido (valor_unitario_pedido * quantidade_pedido)."""
        return pedidos_df.withColumn(
            "valor_total_pedido",
            (F.col("valor_unitario_pedido") * F.col("quantidade_pedido")).cast(DoubleType())
        )

    def get_pagamentos_recusados_legitimos(self, pagamentos_df: DataFrame) -> DataFrame:
        """Retorna pagamentos recusados (status=false) e não fraudulentos (fraude=false)."""
        return pagamentos_df.filter(
            (F.col("status") == False) & (F.col("avaliacao_fraude.fraude") == False)
        )

    def join_pedido_pagamento(self, pedidos_df: DataFrame, pagamentos_df: DataFrame) -> DataFrame:
        """Faz a junção entre os DataFrames de pedidos e pagamentos pelo id_pedido."""
        return (
            pedidos_df.join(pagamentos_df, pedidos_df.id_pedido == pagamentos_df.id_pedido, "inner")
            .select(
                pedidos_df.id_pedido,
                pedidos_df.uf,
                pagamentos_df.forma_pagamento,
                pedidos_df.valor_total_pedido,
                pedidos_df.data_criacao_pedido,
                pagamentos_df.status,
                F.col("avaliacao_fraude.fraude").alias("fraude"),
            )
        )

    def relatorio_pedido_pagamento(self, pagamentos_pedidos: DataFrame) -> DataFrame:
        """Monta o relatório final ordenado por uf, forma_pagamento e data_criacao_pedido."""
        return (
            pagamentos_pedidos
            .select(
                F.col("id_pedido"),
                F.col("uf"),
                F.col("forma_pagamento"),
                F.col("valor_total_pedido"),
                F.col("data_criacao_pedido"),
            )
            .orderBy(F.col("uf"), F.col("forma_pagamento"), F.col("data_criacao_pedido"), ascending=True)
        )
