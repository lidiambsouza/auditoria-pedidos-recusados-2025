# tests/unit/test_transformations.py
import json
import uuid
from datetime import datetime

import pytest
from pyspark.sql.types import (
    DoubleType, FloatType, LongType, StringType,
    StructField, StructType, TimestampType, BooleanType
)

from processing.transformations import Transformation


# --- Helper para criar DataFrames sem o caminho quebrado ---
#
# spark.createDataFrame(lista_python) cria um RDD de objetos picklados e exige
# um worker Python que crasha no Windows com PySpark 4.x (WinError 10038 ao
# fechar o socket). Lemos de um arquivo JSON temporário, que roda inteiramente
# na JVM — o mesmo caminho do pipeline real, que lê de arquivos.

def _to_json_record(value, dtype):
    """Converte uma tupla/valor para JSON respeitando o schema do Spark."""
    if isinstance(dtype, StructType):
        return {
            field.name: _to_json_record(v, field.dataType)
            for field, v in zip(dtype.fields, value)
        }
    if isinstance(dtype, TimestampType):
        return value.isoformat() if value is not None else None
    return value


def make_df(spark, data, schema, tmp_path):
    """Cria um DataFrame a partir de dados locais via arquivo JSON temporário."""
    path = tmp_path / f"{uuid.uuid4().hex}.json"
    with open(path, "w", encoding="utf-8") as f:
        for row in data:
            f.write(json.dumps(_to_json_record(row, schema)) + "\n")
    return spark.read.schema(schema).json(str(path))


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

# Entrada do relatório final = saída de join_pedido_pagamento.
SCHEMA_RELATORIO_IN = StructType([
    StructField("id_pedido", StringType(), False),
    StructField("uf", StringType(), True),
    StructField("forma_pagamento", StringType(), True),
    StructField("valor_total_pedido", DoubleType(), True),
    StructField("data_criacao_pedido", TimestampType(), True),
    StructField("status", BooleanType(), True),
    StructField("fraude", BooleanType(), True),
])


class TestAddValorTotalPedidos:

    def test_calcula_valor_unitario_por_quantidade(self, spark, tmp_path):
        """valor_total deve ser valor_unitario × quantidade."""
        df = make_df(
            spark, [("p1", "TV", 1500.0, 2, None, "SP", 1)], SCHEMA_PEDIDOS, tmp_path,
        )
        resultado = Transformation().add_valor_total_pedidos(df)
        assert resultado.collect()[0].valor_total_pedido == pytest.approx(3000.0)

    def test_adiciona_coluna_valor_total(self, spark, tmp_path):
        """A coluna 'valor_total' deve existir no resultado (etapas seguintes dependem dela)."""
        df = make_df(
            spark, [("p1", "TV", 100.0, 1, None, "SP", 1)], SCHEMA_PEDIDOS, tmp_path,
        )
        resultado = Transformation().add_valor_total_pedidos(df)
        assert "valor_total_pedido" in resultado.columns

    def test_valor_total_zero_quando_quantidade_e_zero(self, spark, tmp_path):
        """Item devolvido (quantidade=0) deve gerar valor_total=0, não erro nem NULL."""
        df = make_df(
            spark, [("p1", "TV", 500.0, 0, None, "SP", 1)], SCHEMA_PEDIDOS, tmp_path,
        )
        resultado = Transformation().add_valor_total_pedidos(df)
        assert resultado.collect()[0].valor_total_pedido == pytest.approx(0.0)

    def test_valor_total_nulo_quando_valor_unitario_e_nulo(self, spark, tmp_path):
        """NULL se propaga em operações aritméticas — comportamento esperado do Spark."""
        df = make_df(
            spark, [("p1", "TV", None, 2, None, "SP", 1)], SCHEMA_PEDIDOS, tmp_path,
        )
        resultado = Transformation().add_valor_total_pedidos(df)
        assert resultado.collect()[0].valor_total_pedido is None


class TestJoinPedidoPagamento:

    @pytest.fixture
    def pedidos_df(self, spark, tmp_path):
        dados = [
            ("p1", "SP", 3000.0, datetime(2025, 1, 15)),
            ("p2", "RJ", 1500.0, datetime(2025, 2, 20)),
        ]
        return make_df(spark, dados, SCHEMA_PEDIDOS_JOIN, tmp_path)

    @pytest.fixture
    def pagamentos_df(self, spark, tmp_path):
        dados = [
            ("p1", "cartao", 3000.0, False, datetime(2025, 1, 15), (False, 0.1)),
            ("p2", "boleto", 1500.0, True, datetime(2025, 2, 20), (False, 0.2)),
        ]
        return make_df(spark, dados, SCHEMA_PAGAMENTOS, tmp_path)

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

    def test_inner_join_exclui_pedido_sem_pagamento(self, spark, tmp_path):
        """Pedido sem pagamento correspondente não deve aparecer no resultado."""
        pedidos = make_df(
            spark, [("p1", "SP", 3000.0, datetime(2025, 1, 15))],
            SCHEMA_PEDIDOS_JOIN, tmp_path,
        )
        pagamentos = make_df(
            spark, [("p99", "pix", 500.0, False, datetime(2025, 1, 10), (False, 0.1))],
            SCHEMA_PAGAMENTOS, tmp_path,
        )
        resultado = Transformation().join_pedido_pagamento(pedidos, pagamentos)
        assert resultado.count() == 0


class TestGetPedidos2025:

    def test_mantem_apenas_pedidos_de_2025(self, spark, tmp_path):
        """Só pedidos com data_criacao_pedido em 2025 devem permanecer."""
        dados = [
            ("p1", "TV", 100.0, 1, datetime(2024, 12, 31), "SP", 1),
            ("p2", "TV", 100.0, 1, datetime(2025, 1, 1), "SP", 2),
            ("p3", "TV", 100.0, 1, datetime(2025, 12, 31), "SP", 3),
            ("p4", "TV", 100.0, 1, datetime(2026, 1, 1), "SP", 4),
        ]
        df = make_df(spark, dados, SCHEMA_PEDIDOS, tmp_path)
        resultado = Transformation().get_pedidos_2025(df)
        ids = {r.id_pedido for r in resultado.collect()}
        assert ids == {"p2", "p3"}

    def test_exclui_pedido_com_data_nula(self, spark, tmp_path):
        """data_criacao_pedido NULL não tem ano 2025 e deve ser descartado."""
        dados = [
            ("p1", "TV", 100.0, 1, datetime(2025, 6, 1), "SP", 1),
            ("p2", "TV", 100.0, 1, None, "SP", 2),
        ]
        df = make_df(spark, dados, SCHEMA_PEDIDOS, tmp_path)
        resultado = Transformation().get_pedidos_2025(df)
        ids = {r.id_pedido for r in resultado.collect()}
        assert ids == {"p1"}


class TestGetPagamentosRecusadosLegitimos:

    def test_mantem_recusado_e_nao_fraudulento(self, spark, tmp_path):
        """status=False (recusado) e fraude=False (legítimo) deve permanecer."""
        dados = [
            ("p1", "cartao", 100.0, False, datetime(2025, 1, 1), (False, 0.1)),
        ]
        df = make_df(spark, dados, SCHEMA_PAGAMENTOS, tmp_path)
        resultado = Transformation().get_pagamentos_recusados_legitimos(df)
        assert resultado.count() == 1
        assert resultado.collect()[0].id_pedido == "p1"

    def test_exclui_pagamento_aprovado(self, spark, tmp_path):
        """status=True (aprovado) não é recusado e deve ser descartado."""
        dados = [
            ("p1", "cartao", 100.0, True, datetime(2025, 1, 1), (False, 0.1)),
        ]
        df = make_df(spark, dados, SCHEMA_PAGAMENTOS, tmp_path)
        resultado = Transformation().get_pagamentos_recusados_legitimos(df)
        assert resultado.count() == 0

    def test_exclui_pagamento_fraudulento(self, spark, tmp_path):
        """fraude=True não é legítimo e deve ser descartado, mesmo se recusado."""
        dados = [
            ("p1", "cartao", 100.0, False, datetime(2025, 1, 1), (True, 0.9)),
        ]
        df = make_df(spark, dados, SCHEMA_PAGAMENTOS, tmp_path)
        resultado = Transformation().get_pagamentos_recusados_legitimos(df)
        assert resultado.count() == 0


class TestRelatorioPedidoPagamento:

    def test_expoe_apenas_colunas_do_relatorio(self, spark, tmp_path):
        """O relatório final deve conter só as 5 colunas — sem status/fraude internos."""
        dados = [
            ("p1", "SP", "cartao", 100.0, datetime(2025, 1, 1), False, False),
        ]
        df = make_df(spark, dados, SCHEMA_RELATORIO_IN, tmp_path)
        resultado = Transformation().relatorio_pedido_pagamento(df)
        assert resultado.columns == [
            "id_pedido", "uf", "forma_pagamento",
            "valor_total_pedido", "data_criacao_pedido",
        ]

    def test_ordena_por_uf_forma_pagamento_e_data(self, spark, tmp_path):
        """As linhas devem sair ordenadas por uf, depois forma_pagamento, depois data."""
        dados = [
            ("p1", "SP", "cartao", 100.0, datetime(2025, 3, 1), False, False),
            ("p2", "RJ", "boleto", 100.0, datetime(2025, 1, 1), False, False),
            ("p3", "SP", "cartao", 100.0, datetime(2025, 1, 1), False, False),
            ("p4", "RJ", "pix", 100.0, datetime(2025, 1, 1), False, False),
        ]
        df = make_df(spark, dados, SCHEMA_RELATORIO_IN, tmp_path)
        resultado = Transformation().relatorio_pedido_pagamento(df)
        ordem = [r.id_pedido for r in resultado.collect()]
        assert ordem == ["p2", "p4", "p3", "p1"]

    def test_remove_pedido_duplicado(self, spark, tmp_path):
        """Pedido com mais de um pagamento (fan-out do join) deve aparecer uma única vez."""
        dados = [
            ("p1", "SP", "cartao", 100.0, datetime(2025, 1, 1), False, False),
            ("p1", "SP", "boleto", 100.0, datetime(2025, 1, 1), False, False),
            ("p2", "RJ", "pix", 200.0, datetime(2025, 2, 1), False, False),
        ]
        df = make_df(spark, dados, SCHEMA_RELATORIO_IN, tmp_path)
        resultado = Transformation().relatorio_pedido_pagamento(df)
        ids = [r.id_pedido for r in resultado.collect()]
        assert sorted(ids) == ["p1", "p2"]
        assert ids.count("p1") == 1


class TestTratamentoDeErros:
    """Cada método deve capturar, logar e propagar erros (try/except + logging)."""

    @pytest.fixture
    def df_invalido(self, spark, tmp_path):
        """DataFrame sem nenhuma das colunas esperadas pelas transformações."""
        schema = StructType([StructField("coluna_inexistente", StringType(), True)])
        return make_df(spark, [("x",)], schema, tmp_path)

    def test_get_pedidos_2025_propaga_erro(self, df_invalido):
        with pytest.raises(Exception):
            Transformation().get_pedidos_2025(df_invalido)

    def test_add_valor_total_propaga_erro(self, df_invalido):
        with pytest.raises(Exception):
            Transformation().add_valor_total_pedidos(df_invalido)

    def test_get_pagamentos_recusados_propaga_erro(self, df_invalido):
        with pytest.raises(Exception):
            Transformation().get_pagamentos_recusados_legitimos(df_invalido)

    def test_join_pedido_pagamento_propaga_erro(self, df_invalido):
        with pytest.raises(Exception):
            Transformation().join_pedido_pagamento(df_invalido, df_invalido)

    def test_relatorio_propaga_erro(self, df_invalido):
        with pytest.raises(Exception):
            Transformation().relatorio_pedido_pagamento(df_invalido)
