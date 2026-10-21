import os
import sys


def test_spark_session_settings(spark):
    assert spark.conf.get("spark.sql.session.timeZone") == "UTC"
    assert spark.conf.get("spark.sql.ansi.enabled") == "false"
    assert spark.conf.get("spark.sql.legacy.timeParserPolicy") == "CORRECTED"


def test_spark_uses_current_python(spark):
    assert os.environ["PYSPARK_PYTHON"] == sys.executable
    assert os.environ["PYSPARK_DRIVER_PYTHON"] == sys.executable
