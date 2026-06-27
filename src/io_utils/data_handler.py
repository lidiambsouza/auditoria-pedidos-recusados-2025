# src/io_utils/data_handler.py
import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    DoubleType,
    StructType,
    StructField,
    StringType,
    FloatType,
    LongType,
    BooleanType,
    TimestampType,
)
from pyspark.errors import AnalysisException

logger = logging.getLogger(__name__)


class DataHandler:
    """Responsável pela leitura e escrita de dados."""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def _get_schema_pagamentos(self) -> StructType:
        return StructType(
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

    def _get_schema_pedidos(self) -> StructType:
        return StructType(
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

    def _get_schema_relatorio(self) -> StructType:
        return StructType(
            [
                StructField("id_pedido", StringType(), False),
                StructField("uf", StringType(), False),
                StructField("forma_pagamento", StringType(), False),
                StructField("valor_total_pedido", DoubleType(), False),
                StructField("data_criacao_pedido", TimestampType(), False),
            ]
        )

    def load_pagamentos(self, path: str, compression: str) -> DataFrame:
        schema = self._get_schema_pagamentos()
        return (
            self.spark.read.schema(schema).option("compression", compression).json(path)
        )

    def load_pedidos(
        self, path: str, compression: str, header: bool, sep: str
    ) -> DataFrame:
        schema = self._get_schema_pedidos()
        return (
            self.spark.read.schema(schema)
            .option("compression", compression)
            .option("header", header)
            .option("sep", sep)
            .csv(path)
        )

    def _schemas_compativeis(self, actual: StructType, expected: StructType) -> bool:
        # Compara apenas nome e tipo — ignora nullable porque o Spark sempre
        # produz nullable=True em colunas de join, independente do schema de entrada.
        if len(actual.fields) != len(expected.fields):
            return False
        return all(
            a.name == e.name and a.dataType == e.dataType
            for a, e in zip(actual.fields, expected.fields)
        )

    def write_parquet(self, df: DataFrame, path: str, validate_schema: bool = False):
        if validate_schema:
            expected = self._get_schema_relatorio()
            if not self._schemas_compativeis(df.schema, expected):
                raise AnalysisException(
                    f"Schema do relatorio diverge do esperado.\n"
                    f"Esperado:  {expected}\n"
                    f"Recebido:  {df.schema}"
                )
        df.write.mode("overwrite").parquet(path)
        logger.info(f"Dados salvos com sucesso em: {path}")
