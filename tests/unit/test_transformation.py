# tests/unit/test_transformations.py
import pytest
from datetime import datetime
from pyspark.sql.types import (
    DoubleType, FloatType, LongType, StringType,
    StructField, StructType, TimestampType, BooleanType
)

from processing.transformations import Transformation


# --- Schemas reutilizáveis ---

SCHEMA_PEDIDOS = StructType(
            [
                StructField("id_pedido", StringType(), False),
                StructField("produto", StringType(), True),
                StructField("valor_unitario_pedido", FloatType(), True),
                StructField("quantidade_pedido", LongType(), True),
                StructField("data_criacao_pedido", TimestampType(), True),
                StructField("uf", StringType(), True),
                StructField("id_cliente", LongType(), True),
            ]
        )

SCHEMA_PEDIDOS_JOIN = StructType([
    StructField("id_pedido", StringType(), False),
    StructField("uf", StringType(), True),
    StructField("valor_total_pedido", DoubleType(), True),
    StructField("data_criacao_pedido", TimestampType(), True),
])

SCHEMA_PAGAMENTOS = StructType(
            [
                StructField("id_pedido", StringType(), False),
                StructField("forma_pagamento", StringType(), True),
                StructField("valor_pagamento", FloatType(), True),
                StructField("status", BooleanType(), True),
                StructField("data_processamento", TimestampType(), True),
                StructField(
                    "avaliacao_fraude",
                    StructType(
                        [
                            StructField("fraude", BooleanType(), True),
                            StructField("score", FloatType(), True),
                        ]
                    ),
                    True,
                ),
            ]
        )


class TestAddValorTotalPedidos:

    def test_calcula_valor_unitario_por_quantidade(self, spark):
        """valor_total deve ser valor_unitario × quantidade."""
        df = spark.createDataFrame(
            [("p1", "TV", 1500.0, 2, None, "SP", 1)], SCHEMA_PEDIDOS,
        )
        resultado = Transformation().add_valor_total_pedidos(df)
        assert resultado.collect()[0].valor_total_pedido == pytest.approx(3000.0)

    def test_adiciona_coluna_valor_total(self, spark):
        """A coluna 'valor_total' deve existir no resultado (etapas seguintes dependem dela)."""
        df = spark.createDataFrame(
            [("p1", "TV", 100.0, 1, None, "SP", 1)], SCHEMA_PEDIDOS,
        )
        resultado = Transformation().add_valor_total_pedidos(df)
        assert "valor_total_pedido" in resultado.columns

    def test_valor_total_zero_quando_quantidade_e_zero(self, spark):
        """Item devolvido (quantidade=0) deve gerar valor_total=0, não erro nem NULL."""
        df = spark.createDataFrame(
            [("p1", "TV", 500.0, 0, None, "SP", 1)], SCHEMA_PEDIDOS,
        )
        resultado = Transformation().add_valor_total_pedidos(df)
        assert resultado.collect()[0].valor_total_pedido == pytest.approx(0.0)

    def test_valor_total_nulo_quando_valor_unitario_e_nulo(self, spark):
        """NULL se propaga em operações aritméticas — comportamento esperado do Spark."""
        df = spark.createDataFrame(
            [("p1", "TV", None, 2, None, "SP", 1)], SCHEMA_PEDIDOS,
        )
        resultado = Transformation().add_valor_total_pedidos(df)
        assert resultado.collect()[0].valor_total_pedido is None


class TestJoinPedidoPagamento:

    @pytest.fixture
    def pedidos_df(self, spark):
        dados = [
            ("p1", "SP", 3000.0, datetime(2025, 1, 15)),
            ("p2", "RJ", 1500.0, datetime(2025, 2, 20)),
        ]
        return spark.createDataFrame(dados, SCHEMA_PEDIDOS_JOIN)

    @pytest.fixture
    def pagamentos_df(self, spark):
        dados = [
            ("p1", "cartao", 3000.0, False, datetime(2025, 1, 15), (False, 0.1)),
            ("p2", "boleto", 1500.0, True, datetime(2025, 2, 20), (False, 0.2)),
        ]
        return spark.createDataFrame(dados, SCHEMA_PAGAMENTOS)

    def test_resultado_contem_colunas_esperadas(self, pedidos_df, pagamentos_df):
        """O join deve retornar exatamente as colunas definidas no contrato."""
        resultado = Transformation().join_pedido_pagamento(pedidos_df, pagamentos_df)
        assert set(resultado.columns) == {
            "id_pedido", "uf", "forma_pagamento",
            "valor_total_pedido", "data_criacao_pedido", "status", "fraude",
        }

    def test_associa_pedido_ao_pagamento_correto(self, pedidos_df, pagamentos_df):
        """Cada id_pedido deve ser ligado à forma de pagamento e UF corretos."""
        resultado = Transformation().join_pedido_pagamento(pedidos_df, pagamentos_df)
        linhas = {r.id_pedido: r for r in resultado.collect()}
        assert linhas["p1"].uf == "SP"
        assert linhas["p1"].forma_pagamento == "cartao"

    def test_inner_join_exclui_pedido_sem_pagamento(self, spark):
        """Pedido sem pagamento correspondente não deve aparecer no resultado."""
        pedidos = spark.createDataFrame(
            [("p1", "SP", 3000.0, datetime(2025, 1, 15))],
            SCHEMA_PEDIDOS_JOIN,
        )
        pagamentos = spark.createDataFrame(
            [("p99", "pix", 500.0, False, datetime(2025, 1, 10), (False, 0.1))],
            SCHEMA_PAGAMENTOS,
        )
        resultado = Transformation().join_pedido_pagamento(pedidos, pagamentos)
        assert resultado.count() == 0