# tests/test_merge_data.py

"""
Tests unitaires pour le module merge_data.py
--------------------------------------------
Vérifie le bon fonctionnement de la fonction merge_home_away()
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from merge_data import merge_home_away


@pytest.fixture(scope="module")
def spark():
    """Initialise une session Spark locale pour les tests."""
    spark = SparkSession.builder \
        .appName("TestMergeData") \
        .master("local[*]") \
        .getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture
def home_away_data(spark):
    """Crée deux DataFrames d'exemple : home et away"""
    df_home = spark.createDataFrame([
        Row(Season=2010, Team="Bayern", TotalHomeWin=1, HomeScoredGoals=3),
        Row(Season=2010, Team="Dortmund", TotalHomeWin=0, HomeScoredGoals=1)
    ])
    df_away = spark.createDataFrame([
        Row(Season=2010, Team="Bayern", TotalAwayWin=2, AwayScoredGoals=3),
        Row(Season=2010, Team="Dortmund", TotalAwayWin=1, AwayScoredGoals=2)
    ])
    return df_home, df_away


def test_merge_home_away_columns_and_values(home_away_data):
    df_home, df_away = home_away_data
    df_merged = merge_home_away(df_home, df_away)

    expected_cols = [
        "Season", "Team", "TotalHomeWin", "HomeScoredGoals",
        "TotalAwayWin", "AwayScoredGoals"
    ]
    assert all(col in df_merged.columns for col in expected_cols)

    # Vérifie valeurs fusionnées pour Bayern
    row = df_merged.filter((df_merged.Team == "Bayern") & (df_merged.Season == 2010)).collect()[0]
    assert row.TotalHomeWin == 1
    assert row.HomeScoredGoals == 3
    assert row.TotalAwayWin == 2
    assert row.AwayScoredGoals == 3


def test_merge_home_away_missing_columns(spark):
    """Vérifie que la fonction lève une erreur si une colonne requise est manquante."""
    df_home = spark.createDataFrame([
        (2010, "Bayern", 1)
    ], ["Season", "Team", "TotalHomeWin"])

    df_away = spark.createDataFrame([
        (2010, "Bayern", 2)
    ], ["Season", "Team", "TotalAwayWin"])

    # Suppression volontaire d'une colonne pour provoquer l'erreur
    df_home_invalid = df_home.drop("Team")

    with pytest.raises(ValueError, match="Colonnes manquantes dans df_home"):
        merge_home_away(df_home_invalid, df_away)
