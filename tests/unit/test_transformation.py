# tests/unit/test_transformations.py
import pytest
from pyspark.sql.types import (
    ArrayType, DateType, FloatType, LongType, StringType,
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

SCHEMA_PEDIDOS_COM_TOTAL = StructType([
    StructField("id_cliente", LongType(), True),
    StructField("valor_total", FloatType(), True),
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
        assert resultado.collect()[0].valor_total == pytest.approx(3000.0)

    def test_adiciona_coluna_valor_total(self, spark):
        """A coluna 'valor_total' deve existir no resultado (etapas seguintes dependem dela)."""
        df = spark.createDataFrame(
            [("p1", "TV", 100.0, 1, None, "SP", 1)], SCHEMA_PEDIDOS,
        )
        resultado = Transformation().add_valor_total_pedidos(df)
        assert "valor_total" in resultado.columns

    def test_valor_total_zero_quando_quantidade_e_zero(self, spark):
        """Item devolvido (quantidade=0) deve gerar valor_total=0, não erro nem NULL."""
        df = spark.createDataFrame(
            [("p1", "TV", 500.0, 0, None, "SP", 1)], SCHEMA_PEDIDOS,
        )
        resultado = Transformation().add_valor_total_pedidos(df)
        assert resultado.collect()[0].valor_total == pytest.approx(0.0)

    def test_valor_total_nulo_quando_valor_unitario_e_nulo(self, spark):
        """NULL se propaga em operações aritméticas — comportamento esperado do Spark."""
        df = spark.createDataFrame(
            [("p1", "TV", None, 2, None, "SP", 1)], SCHEMA_PEDIDOS,
        )
        resultado = Transformation().add_valor_total_pedidos(df)
        assert resultado.collect()[0].valor_total is None


class TestJoinPedidosPagamentos:

    @pytest.fixture
    def pedidos_df(self, spark):
        return spark.createDataFrame([(1, 1500.0), (2, 300.0)], SCHEMA_PEDIDOS_COM_TOTAL)

    @pytest.fixture
    def clientes_df(self, spark):
        dados = [
            (1, "Ana Lima", None, "000.000.000-00", "ana@test.com", None),
            (2, "Carlos Melo", None, "111.111.111-11", "carlos@test.com", None),
        ]
        return spark.createDataFrame(dados, SCHEMA_PAGAMENTOS)

    def test_resultado_contem_apenas_as_colunas_esperadas(self, spark, pedidos_df, clientes_df):
        """O relatório deve expor só id_cliente, nome, email e valor_total — nada de CPF/data_nasc."""
        resultado = Transformation().join_pedidos_clientes(pedidos_df, clientes_df)
        assert set(resultado.columns) == {"id_cliente", "nome", "email", "valor_total"}

    def test_associa_cliente_correto_ao_pedido(self, spark, pedidos_df, clientes_df):
        """Cada id_cliente deve ser ligado ao nome e email corretos."""
        resultado = Transformation().join_pedidos_clientes(pedidos_df, clientes_df)
        linhas = {r.id_cliente: r for r in resultado.collect()}
        assert linhas[1].nome == "Ana Lima"
        assert linhas[1].email == "ana@test.com"

    def test_inner_join_exclui_cliente_sem_pedido(self, spark):
        """Cliente sem pedido não deve aparecer. Um LEFT JOIN poluiria o relatório com valor_total NULL."""
        pedidos = spark.createDataFrame([(1, 1500.0)], SCHEMA_PEDIDOS_COM_TOTAL)
        clientes = spark.createDataFrame(
            [
                (1, "Ana Lima", None, "000.000.000-00", "ana@test.com", None),
                (99, "Sem Pedido", None, "999.999.999-99", "x@test.com", None),
            ],
            SCHEMA_PAGAMENTOS,
        )
        resultado = Transformation().join_pedidos_clientes(pedidos, clientes)
        assert resultado.count() == 1
        assert resultado.collect()[0].nome == "Ana Lima"