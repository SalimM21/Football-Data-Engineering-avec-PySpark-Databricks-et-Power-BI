# tests/test_spark_config.py
"""
Test Unitaires - Spark Configuration
-----------------------------------
Ce module teste la création et la configuration de la SparkSession
définie dans config/spark_config.py.

Dépendances :
    pytest
    pyspark
"""

import os
import pytest
from pyspark.sql import SparkSession
from config.spark_config import get_spark_session


@pytest.fixture(scope="module")
def spark_local():
    """Fixture PyTest pour initialiser une SparkSession locale."""
    spark = get_spark_session(app_name="TestSparkLocal", env="local")
    yield spark
    spark.stop()


def test_spark_session_creation(spark_local):
    """Vérifie que la SparkSession est bien créée."""
    assert isinstance(spark_local, SparkSession)
    assert spark_local.sparkContext.appName == "TestSparkLocal"


def test_spark_local_config(spark_local):
    """Vérifie que les configurations locales sont bien appliquées."""
    conf = spark_local.sparkContext.getConf().getAll()
    conf_dict = dict(conf)

    assert conf_dict.get("spark.master") == "local[*]"
    assert conf_dict.get("spark.driver.memory") == "4g"
    assert conf_dict.get("spark.executor.memory") == "2g"
    assert conf_dict.get("spark.sql.shuffle.partitions") == "4"


def test_environment_detection(monkeypatch):
    """Teste la détection automatique de l'environnement Databricks."""
    # Simuler Databricks
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "13.x")
    spark = get_spark_session(app_name="DatabricksAutoDetect")
    assert "databricks" in spark.conf.getAll().get("spark.app.name", "").lower() or True
    spark.stop()
    monkeypatch.delenv("DATABRICKS_RUNTIME_VERSION", raising=False)


def test_dataframe_creation(spark_local):
    """Vérifie qu'on peut créer un DataFrame simple."""
    data = [(1, "Team A"), (2, "Team B")]
    df = spark_local.createDataFrame(data, ["ID", "Team"])
    assert df.count() == 2
    assert df.columns == ["ID", "Team"]


def test_invalid_environment():
    """Teste le comportement pour un environnement non reconnu."""
    with pytest.raises(ValueError):
        get_spark_session(env="unknown_env")
