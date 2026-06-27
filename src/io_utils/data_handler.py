# src/io_utils/data_handler.py
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField,
    StringType, FloatType, LongType,
    BooleanType, TimestampType,
)


class DataHandler:
    """Responsável pela leitura e escrita de dados."""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def _get_schema_pagamentos(self) -> StructType:
        return StructType([
            StructField("id_pedido",          StringType(),    False),
            StructField("forma_pagamento",    StringType(),    True),
            StructField("valor_pagamento",    FloatType(),     True),
            StructField("status",             BooleanType(),   True),
            StructField("data_processamento", TimestampType(), True),
            StructField("avaliacao_fraude", StructType([
                StructField("fraude", BooleanType(), True),
                StructField("score",  FloatType(),   True),
            ]), True),
        ])

    def _get_schema_pedidos(self) -> StructType:
        return StructType([
            StructField("id_pedido",      StringType(),    False),
            StructField("produto",        StringType(),    True),
            StructField("valor_unitario", FloatType(),     True),
            StructField("quantidade",     LongType(),      True),
            StructField("data_criacao",   TimestampType(), True),
            StructField("uf",             StringType(),    True),
            StructField("id_cliente",     LongType(),      True),
        ])

    def load_pagamentos(self, path: str, compression: str) -> DataFrame:
        schema = self._get_schema_pagamentos()
        return (
            self.spark.read
            .schema(schema)
            .option("compression", compression)
            .json(path)
        )

    def load_pedidos(self, path: str, compression: str, header: bool, sep: str) -> DataFrame:
        schema = self._get_schema_pedidos()
        return (
            self.spark.read
            .schema(schema)
            .option("compression", compression)
            .option("header", header)
            .option("sep", sep)
            .csv(path)
        )

    def write_parquet(self, df: DataFrame, path: str):
        df.write.mode("overwrite").parquet(path)
        print(f"Dados salvos com sucesso em: {path}")
