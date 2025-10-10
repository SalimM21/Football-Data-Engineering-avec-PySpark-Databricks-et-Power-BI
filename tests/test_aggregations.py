# tests/test_aggregations.py

"""
Tests unitaires pour le module aggregations.py
---------------------------------------------
Vérifie le bon fonctionnement des fonctions :
- aggregate_home_stats()
- aggregate_away_stats()
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from aggregations import aggregate_home_stats, aggregate_away_stats


@pytest.fixture(scope="module")
def spark():
    """Initialise une session Spark locale pour les tests."""
    spark = SparkSession.builder \
        .appName("TestAggregations") \
        .master("local[*]") \
        .getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture
def sample_data(spark):
    """Crée un petit DataFrame d'exemple pour les tests."""
    data = [
        Row(Season=2010, HomeTeam="Bayern", AwayTeam="Dortmund",
            HomeTeamWin=1, AwayTeamWin=0, GameTie=0, FTHG=3, FTAG=1),
        Row(Season=2010, HomeTeam="Dortmund", AwayTeam="Bayern",
            HomeTeamWin=0, AwayTeamWin=1, GameTie=0, FTHG=1, FTAG=2),
        Row(Season=2010, HomeTeam="Leverkusen", AwayTeam="Bayern",
            HomeTeamWin=0, AwayTeamWin=1, GameTie=0, FTHG=0, FTAG=1),
        Row(Season=2011, HomeTeam="Bayern", AwayTeam="Leverkusen",
            HomeTeamWin=1, AwayTeamWin=0, GameTie=0, FTHG=4, FTAG=0),
        Row(Season=2011, HomeTeam="Dortmund", AwayTeam="Bayern",
            HomeTeamWin=1, AwayTeamWin=0, GameTie=0, FTHG=3, FTAG=1),
    ]
    return spark.createDataFrame(data)


def test_aggregate_home_stats_columns_and_values(sample_data):
    """Teste la structure et les valeurs des statistiques à domicile."""
    df_home = aggregate_home_stats(sample_data)

    expected_cols = [
        "Season", "Team", "TotalHomeWin", "TotalHomeLoss",
        "TotalHomeTie", "HomeScoredGoals", "HomeAgainstGoals"
    ]
    assert all(col in df_home.columns for col in expected_cols)

    # Vérifie les valeurs pour Bayern saison 2010
    row = df_home.filter((df_home.Team == "Bayern") & (df_home.Season == 2010)).collect()[0]
    assert row.TotalHomeWin == 1
    assert row.HomeScoredGoals == 3
    assert row.HomeAgainstGoals == 1


def test_aggregate_away_stats_columns_and_values(sample_data):
    """Teste la structure et les valeurs des statistiques à l'extérieur."""
    df_away = aggregate_away_stats(sample_data)

    expected_cols = [
        "Season", "Team", "TotalAwayWin", "TotalAwayLoss",
        "TotalAwayTie", "AwayScoredGoals", "AwayAgainstGoals"
    ]
    assert all(col in df_away.columns for col in expected_cols)

    # Vérifie les valeurs pour Bayern saison 2010 (2 matchs à l'extérieur)
    row = df_away.filter((df_away.Team == "Bayern") & (df_away.Season == 2010)).collect()[0]
    assert row.TotalAwayWin == 2
    assert row.AwayScoredGoals == 3
    assert row.AwayAgainstGoals == 1


def test_aggregate_home_stats_missing_columns(spark):
    """Teste le comportement si une colonne requise est manquante."""
    df_missing = spark.createDataFrame([
        (2010, "Bayern", 1, 0),
    ], ["Season", "HomeTeam", "FTHG", "FTAG"])

    with pytest.raises(ValueError, match="Colonne manquante"):
        aggregate_home_stats(df_missing)


def test_aggregate_away_stats_missing_columns(spark):
    """Teste le comportement si une colonne requise est manquante."""
    df_missing = spark.createDataFrame([
        (2010, "Dortmund", 1, 0),
    ], ["Season", "AwayTeam", "FTAG", "FTHG"])

    with pytest.raises(ValueError, match="Colonne manquante"):
        aggregate_away_stats(df_missing)
