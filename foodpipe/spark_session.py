import os

from pyspark.sql import SparkSession


def get_spark(app_name: str) -> SparkSession:
    session = (
        SparkSession.builder
        .appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("WARN")
    return session
