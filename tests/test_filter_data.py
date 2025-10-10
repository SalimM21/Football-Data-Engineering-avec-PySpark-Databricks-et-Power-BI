# tests/test_filter_data.py

"""
Tests unitaires pour le module filter_data.py
--------------------------------------------
Vérifie le bon fonctionnement du filtrage :
- Div = 'D1' uniquement
- Season entre 2000 et 2015 inclus
"""

import pytest
from pyspark.sql import SparkSession
from filter_data import filter_bundesliga_data


@pytest.fixture(scope="module")
def spark():
    """Initialise une session Spark pour les tests."""
    spark = SparkSession.builder \
        .appName("TestFilterData") \
        .master("local[*]") \
        .getOrCreate()
    yield spark
    spark.stop()


def test_filter_bundesliga_data_basic(spark):
    """Vérifie que le filtrage conserve uniquement la Bundesliga entre 2000 et 2015."""
    data = [
        ("D1", 2001, "TeamA", "TeamB"),   # ✅ Gardé
        ("D2", 2005, "TeamC", "TeamD"),   # ❌ Autre division
        ("D1", 2017, "TeamE", "TeamF"),   # ❌ Hors plage
        ("D1", 2010, "TeamG", "TeamH")    # ✅ Gardé
    ]
    cols = ["Div", "Season", "HomeTeam", "AwayTeam"]
    df = spark.createDataFrame(data, cols)

    df_filtered = filter_bundesliga_data(df).collect()

    assert len(df_filtered) == 2  # Deux lignes devraient être conservées
    assert all(row["Div"] == "D1" for row in df_filtered)
    assert all(2000 <= row["Season"] <= 2015 for row in df_filtered)


def test_filter_bundesliga_data_missing_columns(spark):
    """Vérifie que la fonction lève une erreur si les colonnes nécessaires sont absentes."""
    data = [("D1", "TeamA"), ("D2", "TeamB")]
    cols = ["Div", "HomeTeam"]
    df = spark.createDataFrame(data, cols)

    with pytest.raises(ValueError, match="Colonnes manquantes"):
        filter_bundesliga_data(df)
