# src/main.py
from config.settings import carregar_config
from session.spark_session import SparkSessionManager
from processing.transformations import Transformation
from pipeline.pipeline import Pipeline

def main():
    config = carregar_config()

    app_name          = config["spark"]["app_name"]
    print(f"Obtido o app name: {app_name}")


    print("Abrindo a sessao spark")
    spark = SparkSessionManager.get_spark_session(app_name=app_name)    
    transformer = Transformation()

    pipeline = Pipeline(spark, transformer)
    pipeline.run(config=config)

    spark.stop()

if __name__ == "__main__":
  main()