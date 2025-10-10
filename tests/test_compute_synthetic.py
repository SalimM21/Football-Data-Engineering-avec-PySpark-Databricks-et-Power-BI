# tests/test_compute_synthetic.py

"""
Tests unitaires pour le module compute_synthetic.py
---------------------------------------------------
Vérifie le bon fonctionnement de compute_synthetic_stats()
"""

import pytest
from pyspark.sql import SparkSession, Row
from compute_synthetic import compute_synthetic_stats


@pytest.fixture(scope="module")
def spark():
    """Initialise une session Spark locale pour les tests."""
    spark = SparkSession.builder \
        .appName("TestComputeSynthetic") \
        .master("local[*]") \
        .getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture
def sample_data(spark):
    """Crée un mini DataFrame pour les tests de colonnes synthétiques."""
    data = [
        Row(
            Season=2010,
            Team="Bayern",
            TotalHomeWin=1,
            HomeScoredGoals=3,
            TotalHomeLoss=0,
            TotalHomeTie=0,
            TotalAwayWin=2,
            AwayScoredGoals=3,
            TotalAwayLoss=1,
            TotalAwayTie=0
        ),
        Row(
            Season=2010,
            Team="Dortmund",
            TotalHomeWin=0,
            HomeScoredGoals=1,
            TotalHomeLoss=1,
            TotalHomeTie=0,
            TotalAwayWin=1,
            AwayScoredGoals=2,
            TotalAwayLoss=0,
            TotalAwayTie=0
        ),
        Row(
            Season=2011,
            Team="Leverkusen",
            TotalHomeWin=0,
            HomeScoredGoals=0,
            TotalHomeLoss=0,
            TotalHomeTie=0,
            TotalAwayWin=0,
            AwayScoredGoals=0,
            TotalAwayLoss=0,
            TotalAwayTie=0
        )
    ]
    return spark.createDataFrame(data)


def test_columns_presence(sample_data):
    """Vérifie que toutes les colonnes synthétiques et avancées sont présentes."""
    df_synthetic = compute_synthetic_stats(sample_data)
    expected_cols = [
        "GoalsScored", "GoalsAgainst", "Win", "Loss", "Tie",
        "TotalGames", "GoalDifferentials", "WinPercentage",
        "GoalsPerGame", "GoalsAgainstPerGame"
    ]
    assert all(col in df_synthetic.columns for col in expected_cols)


def test_values_computation(sample_data):
    """Vérifie les calculs totaux et avancés pour une équipe donnée."""
    df_synthetic = compute_synthetic_stats(sample_data)

    # Bayern
    row = df_synthetic.filter(df_synthetic.Team == "Bayern").collect()[0]
    assert row.GoalsScored == 6  # 3 + 3
    assert row.GoalsAgainst == 1  # 0 + 1
    assert row.Win == 3  # 1 + 2
    assert row.Loss == 1  # 0 + 1
    assert row.Tie == 0  # 0 + 0
    assert row.TotalGames == 4  # 3 + 1 + 0
    assert row.GoalDifferentials == 5  # 6 - 1
    assert row.WinPercentage == pytest.approx(3 / 4)
    assert row.GoalsPerGame == pytest.approx(6 / 4)
    assert row.GoalsAgainstPerGame == pytest.approx(1 / 4)

    # Leverkusen (matchs = 0)
    row_zero = df_synthetic.filter(df_synthetic.Team == "Leverkusen").collect()[0]
    assert row_zero.TotalGames == 0
    assert row_zero.WinPercentage == 0
    assert row_zero.GoalsPerGame == 0
    assert row_zero.GoalsAgainstPerGame == 0
