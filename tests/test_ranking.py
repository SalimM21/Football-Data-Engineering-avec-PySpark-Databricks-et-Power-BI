# tests/test_ranking.py

"""
Tests unitaires pour le module ranking.py
-----------------------------------------
Vérifie le bon fonctionnement de la fonction rank_teams()
"""

import pytest
from pyspark.sql import SparkSession, Row
from ranking import rank_teams


@pytest.fixture(scope="module")
def spark():
    """Initialise une session Spark locale pour les tests."""
    spark = SparkSession.builder \
        .appName("TestRanking") \
        .master("local[*]") \
        .getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture
def sample_data(spark):
    """Crée un mini DataFrame pour tester le classement des équipes."""
    data = [
        Row(Season=2010, Team="Bayern", WinPercentage=0.75, GoalDifferentials=5),
        Row(Season=2010, Team="Dortmund", WinPercentage=0.50, GoalDifferentials=2),
        Row(Season=2010, Team="Leverkusen", WinPercentage=0.50, GoalDifferentials=3),
        Row(Season=2011, Team="Bayern", WinPercentage=0.60, GoalDifferentials=4),
        Row(Season=2011, Team="Dortmund", WinPercentage=0.80, GoalDifferentials=6)
    ]
    return spark.createDataFrame(data)


def test_team_position_presence(sample_data):
    """Vérifie que la colonne TeamPosition est créée."""
    df_ranked = rank_teams(sample_data)
    assert "TeamPosition" in df_ranked.columns


def test_team_position_values(sample_data):
    """Vérifie que le classement est correct par saison."""
    df_ranked = rank_teams(sample_data)
    result_2010 = df_ranked.filter(df_ranked.Season == 2010).collect()

    # Bayern devrait être 1
    bayern_row = [r for r in result_2010 if r.Team == "Bayern"][0]
    assert bayern_row.TeamPosition == 1

    # Leverkusen avant Dortmund car GoalDifferentials plus élevé malgré même WinPercentage
    leverkusen_row = [r for r in result_2010 if r.Team == "Leverkusen"][0]
    dortmund_row = [r for r in result_2010 if r.Team == "Dortmund"][0]
    assert leverkusen_row.TeamPosition < dortmund_row.TeamPosition


def test_missing_columns_error(spark):
    """Vérifie qu'une erreur est levée si des colonnes requises sont manquantes."""
    df_invalid = spark.createDataFrame([
        (2010, "Bayern", 0.75)
    ], ["Season", "Team", "WinPercentage"])  # GoalDifferentials manquant

    with pytest.raises(ValueError, match="Colonnes manquantes dans df"):
        rank_teams(df_invalid)
