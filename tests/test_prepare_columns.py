# tests/test_prepare_columns.py
"""
===========================================================
 Module de test : test_prepare_columns.py
 Projet : Football Performance Analytics (PySpark)
 Objectif : Vérifier la bonne création et cohérence
            des colonnes indicatrices de résultats.
===========================================================
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from src.prepare_columns import add_result_indicators, verify_result_consistency, prepare_match_indicators


@pytest.fixture(scope="module")
def spark():
    """Initialise une session Spark pour les tests."""
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("TestPrepareColumns")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture
def sample_df(spark):
    """Crée un DataFrame de test minimal avec les résultats de match."""
    data = [
        Row(Match_ID=1, HomeTeam="Bayern", AwayTeam="Dortmund", FinalResult="H"),
        Row(Match_ID=2, HomeTeam="Leverkusen", AwayTeam="Bayern", FinalResult="A"),
        Row(Match_ID=3, HomeTeam="Chelsea", AwayTeam="Arsenal", FinalResult="D"),
    ]
    return spark.createDataFrame(data)


def test_add_result_indicators(sample_df):
    """Vérifie que les colonnes indicatrices sont correctement ajoutées."""
    df = add_result_indicators(sample_df)

    expected_columns = {"HomeTeamWin", "AwayTeamWin", "GameTie"}
    actual_columns = set(df.columns)

    # Vérifie la présence des nouvelles colonnes
    assert expected_columns.issubset(actual_columns), "Les colonnes indicatrices ne sont pas toutes présentes."

    # Vérifie quelques valeurs
    rows = df.select("FinalResult", "HomeTeamWin", "AwayTeamWin", "GameTie").collect()
    for row in rows:
        if row["FinalResult"] == "H":
            assert row["HomeTeamWin"] == 1
            assert row["AwayTeamWin"] == 0
            assert row["GameTie"] == 0
        elif row["FinalResult"] == "A":
            assert row["HomeTeamWin"] == 0
            assert row["AwayTeamWin"] == 1
            assert row["GameTie"] == 0
        elif row["FinalResult"] == "D":
            assert row["HomeTeamWin"] == 0
            assert row["AwayTeamWin"] == 0
            assert row["GameTie"] == 1


def test_verify_result_consistency_valid(sample_df):
    """Vérifie qu’aucune erreur n’est levée pour des données cohérentes."""
    df = add_result_indicators(sample_df)
    # Ne doit pas lever d’exception
    verify_result_consistency(df)


def test_verify_result_consistency_invalid(spark):
    """Vérifie qu’une erreur est levée pour des données incohérentes."""
    # Deux colonnes à 1 sur une même ligne
    data = [
        Row(Match_ID=1, HomeTeamWin=1, AwayTeamWin=1, GameTie=0),
        Row(Match_ID=2, HomeTeamWin=0, AwayTeamWin=0, GameTie=1),
    ]
    df_invalid = spark.createDataFrame(data)

    with pytest.raises(ValueError, match="Incohérence détectée"):
        verify_result_consistency(df_invalid)


def test_prepare_match_indicators_pipeline(sample_df):
    """Test complet du pipeline prepare_match_indicators."""
    df_final = prepare_match_indicators(sample_df)

    # Vérifie que les colonnes ont été ajoutées
    assert "HomeTeamWin" in df_final.columns
    assert "AwayTeamWin" in df_final.columns
    assert "GameTie" in df_final.columns

    # Vérifie qu’aucune incohérence n’existe
    total_flags = df_final.selectExpr("HomeTeamWin + AwayTeamWin + GameTie AS sum_flags").collect()
    assert all(row["sum_flags"] == 1 for row in total_flags), "Une incohérence a été détectée dans le pipeline."
