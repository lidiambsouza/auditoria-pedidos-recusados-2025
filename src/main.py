# src/main.py
import logging
import sys

from config.settings import carregar_config
from session.spark_session import SparkSessionManager
from processing.transformations import Transformation
from pipeline.pipeline import Pipeline

logger = logging.getLogger(__name__)


def main():
    config = carregar_config()

    app_name = config["spark"]["app_name"]
    logger.info(f"Obtido o app name: {app_name}")

    logger.info("Abrindo a sessao spark")
    spark = SparkSessionManager.get_spark_session(app_name=app_name)
    transformer = Transformation()

    pipeline = Pipeline(spark, transformer)
    pipeline.run(config=config)

    spark.stop()


# Crie a configuração do logging
def configurar_logging():
    """Configura o logging para todo o projeto."""

    # No Windows o console usa cp1252 por padrão; força UTF-8 para exibir acentos corretamente.
    # O hasattr evita erro quando stdout é redirecionado (pipe, spark-submit, CI/CD).
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    logging.basicConfig(
        # Nível mínimo de severidade para ser registrado.
        # DEBUG < INFO < WARNING < ERROR < CRITICAL
        level=logging.INFO,
        # Formato da mensagem de log.
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        # Lista de handlers. Aqui, estamos logando para um arquivo e para o console.
        handlers=[
            logging.FileHandler(
                "auditoria-pedidos-recusados-2025.log", encoding="utf-8"
            ),  # Log para arquivo
            logging.StreamHandler(sys.stdout),  # Log para o console (terminal)
        ],
    )
    logging.info("Logging configurado.")


if __name__ == "__main__":
    configurar_logging()
    main()
