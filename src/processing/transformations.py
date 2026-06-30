# src/processing/transformations.py
import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

logger = logging.getLogger(__name__)


class Transformation:
    """Contém as transformações e regras de negócio da aplicação."""

    def get_pedidos_2025(self, pedidos_df: DataFrame) -> DataFrame:
        """Retorna apenas os pedidos do ano de 2025."""
        try:
            logger.info("Filtrando pedidos do ano de 2025.")
            return pedidos_df.filter(F.year(F.col("data_criacao_pedido")) == 2025)
        except Exception as e:
            logger.error(f"Erro ao filtrar pedidos de 2025: {e}")
            raise

    def add_valor_total_pedidos(self, pedidos_df: DataFrame) -> DataFrame:
        """Adiciona a coluna valor_total_pedido (valor_unitario_pedido * quantidade_pedido)."""
        try:
            logger.info("Calculando o valor total dos pedidos.")
            return pedidos_df.withColumn(
                "valor_total_pedido",
                (F.col("valor_unitario_pedido") * F.col("quantidade_pedido")).cast(
                    DoubleType()
                ),
            )
        except Exception as e:
            logger.error(f"Erro ao adicionar o valor total dos pedidos: {e}")
            raise

    def get_pagamentos_recusados_legitimos(self, pagamentos_df: DataFrame) -> DataFrame:
        """Retorna pagamentos recusados (status=false) e não fraudulentos (fraude=false)."""
        try:
            logger.info("Filtrando pagamentos recusados e não fraudulentos.")
            return pagamentos_df.filter(
                ~F.col("status") & ~F.col("avaliacao_fraude.fraude")
            )
        except Exception as e:
            logger.error(f"Erro ao filtrar pagamentos recusados legítimos: {e}")
            raise

    def join_pedido_pagamento(
        self, pedidos_df: DataFrame, pagamentos_df: DataFrame
    ) -> DataFrame:
        """Faz a junção entre os DataFrames de pedidos e pagamentos pelo id_pedido."""
        try:
            logger.info("Realizando o join entre pedidos e pagamentos.")
            return pedidos_df.join(
                pagamentos_df, pedidos_df.id_pedido == pagamentos_df.id_pedido, "inner"
            ).select(
                pedidos_df.id_pedido,
                pedidos_df.uf,
                pagamentos_df.forma_pagamento,
                pedidos_df.valor_total_pedido,
                pedidos_df.data_criacao_pedido,
                pagamentos_df.status,
                F.col("avaliacao_fraude.fraude").alias("fraude"),
            )
        except Exception as e:
            logger.error(f"Erro ao unir pedidos e pagamentos: {e}")
            raise

    def relatorio_pedido_pagamento(self, pagamentos_pedidos: DataFrame) -> DataFrame:
        """Monta o relatório final ordenado por uf, forma_pagamento e data_criacao_pedido."""
        try:
            logger.info("Montando o relatório final de pedidos e pagamentos.")
            return (
                pagamentos_pedidos.select(
                    F.col("id_pedido"),
                    F.col("uf"),
                    F.col("forma_pagamento"),
                    F.col("valor_total_pedido"),
                    F.col("data_criacao_pedido"),
                )
                # Protege contra duplicatas: um pedido com mais de um pagamento
                # recusado/legítimo geraria fan-out no join. Garante 1 linha por pedido.
                .dropDuplicates(["id_pedido"])
                .orderBy(
                    F.col("uf"),
                    F.col("forma_pagamento"),
                    F.col("data_criacao_pedido"),
                    ascending=True,
                )
            )
        except Exception as e:
            logger.error(f"Erro ao montar o relatório final: {e}")
            raise
