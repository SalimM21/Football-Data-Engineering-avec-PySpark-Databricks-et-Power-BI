# tests/test_indicators.py

"""
Tests unitaires pour le module indicators.py
--------------------------------------------
Vérifie la création correcte des colonnes :
- HomeTeamWin
- AwayTeamWin
- GameTie
"""

import pytest
from pyspark.sql import SparkSession
from indicators import add_match_indicators


@pytest.fixture(scope="module")
def spark():
    """Initialise une session Spark pour les tests."""
    spark = SparkSession.builder \
        .appName("TestIndicators") \
        .master("local[*]") \
        .getOrCreate()
    yield spark
    spark.stop()


def test_add_match_indicators_structure(spark):
    """Vérifie que les colonnes indicatrices sont bien ajoutées au DataFrame."""
    data = [
        ("TeamA", "TeamB", 3, 1),
        ("TeamC", "TeamD", 0, 2),
        ("TeamE", "TeamF", 1, 1)
    ]
    columns = ["HomeTeam", "AwayTeam", "FTHG", "FTAG"]

    df = spark.createDataFrame(data, columns)
    df_result = add_match_indicators(df)

    expected_cols = set(columns + ["HomeTeamWin", "AwayTeamWin", "GameTie"])
    assert expected_cols.issubset(set(df_result.columns))


def test_add_match_indicators_values(spark):
    """Vérifie la logique des colonnes HomeTeamWin, AwayTeamWin, GameTie."""
    data = [
        ("TeamA", "TeamB", 2, 1),  # victoire domicile
        ("TeamC", "TeamD", 1, 3),  # victoire extérieur
        ("TeamE", "TeamF", 2, 2)   # match nul
    ]
    columns = ["HomeTeam", "AwayTeam", "FTHG", "FTAG"]
    df = spark.createDataFrame(data, columns)

    df_result = add_match_indicators(df).collect()

    # Vérifie les valeurs attendues
    assert df_result[0]["HomeTeamWin"] == 1
    assert df_result[0]["AwayTeamWin"] == 0
    assert df_result[0]["GameTie"] == 0

    assert df_result[1]["HomeTeamWin"] == 0
    assert df_result[1]["AwayTeamWin"] == 1
    assert df_result[1]["GameTie"] == 0

    assert df_result[2]["HomeTeamWin"] == 0
    assert df_result[2]["AwayTeamWin"] == 0
    assert df_result[2]["GameTie"] == 1
