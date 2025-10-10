# src/aggregations.py

"""
Module : aggregations.py
-------------------------
Ce module regroupe les fonctions d'agrégation PySpark pour le calcul
des statistiques à domicile et à l'extérieur dans les matchs de football.

Étape 4 du pipeline :
    - Calcul des victoires, défaites, nuls et buts marqués/encaissés
    - Agrégation par équipe et par saison
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def aggregate_home_stats(df: DataFrame) -> DataFrame:
    """
    Calcule les statistiques à domicile par équipe et saison.

    Args:
        df (DataFrame): DataFrame contenant les colonnes HomeTeamWin, GameTie, FTHG, FTAG, Season, HomeTeam.

    Returns:
        DataFrame: Résumé par (Season, HomeTeam) avec les totaux à domicile.
    """
    required_cols = ["HomeTeam", "Season", "HomeTeamWin", "GameTie", "FTHG", "FTAG"]
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Colonne manquante : {col}")

    df_home = (
        df.groupBy("Season", "HomeTeam")
        .agg(
            F.sum("HomeTeamWin").alias("TotalHomeWin"),
            F.sum(F.when(F.col("HomeTeamWin") == 0, 1).otherwise(0)).alias("TotalHomeLoss"),
            F.sum("GameTie").alias("TotalHomeTie"),
            F.sum("FTHG").alias("HomeScoredGoals"),
            F.sum("FTAG").alias("HomeAgainstGoals"),
        )
        .withColumnRenamed("HomeTeam", "Team")
    )

    return df_home


def aggregate_away_stats(df: DataFrame) -> DataFrame:
    """
    Calcule les statistiques à l'extérieur par équipe et saison.

    Args:
        df (DataFrame): DataFrame contenant les colonnes AwayTeamWin, GameTie, FTHG, FTAG, Season, AwayTeam.

    Returns:
        DataFrame: Résumé par (Season, AwayTeam) avec les totaux à l'extérieur.
    """
    required_cols = ["AwayTeam", "Season", "AwayTeamWin", "GameTie", "FTHG", "FTAG"]
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Colonne manquante : {col}")

    df_away = (
        df.groupBy("Season", "AwayTeam")
        .agg(
            F.sum("AwayTeamWin").alias("TotalAwayWin"),
            F.sum(F.when(F.col("AwayTeamWin") == 0, 1).otherwise(0)).alias("TotalAwayLoss"),
            F.sum("GameTie").alias("TotalAwayTie"),
            F.sum("FTAG").alias("AwayScoredGoals"),
            F.sum("FTHG").alias("AwayAgainstGoals"),
        )
        .withColumnRenamed("AwayTeam", "Team")
    )

    return df_away
