def test_spark_session_settings(spark):
    assert spark.conf.get("spark.sql.session.timeZone") == "UTC"
    assert spark.conf.get("spark.sql.ansi.enabled") == "true"
