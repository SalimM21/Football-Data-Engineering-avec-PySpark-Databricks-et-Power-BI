# src/prepare_columns.py
"""
===========================================================
 Module : prepare_columns.py
 Projet : Football Performance Analytics (PySpark)
 Étape 2 : Création des colonnes indicatrices
===========================================================

🎯 Objectif :
Ajouter des colonnes dérivées basées sur le résultat final du match :
    - HomeTeamWin : 1 si victoire de l'équipe à domicile, 0 sinon
    - AwayTeamWin : 1 si victoire de l'équipe à l'extérieur, 0 sinon
    - GameTie     : 1 si match nul, 0 sinon

Les colonnes sont ensuite validées pour s'assurer de leur cohérence :
    - Une seule des trois colonnes doit valoir 1 par match.
    - Aucun match ne doit être non catégorisé.
-----------------------------------------------------------
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_result_indicators(df: DataFrame) -> DataFrame:
    """
    Ajoute les colonnes indicatrices HomeTeamWin, AwayTeamWin et GameTie
    en fonction de la colonne FinalResult (H, A, D).

    Args:
        df (DataFrame): DataFrame contenant la colonne 'FinalResult'

    Returns:
        DataFrame: DataFrame enrichi avec les nouvelles colonnes.
    """
    df_indicators = (
        df.withColumn("HomeTeamWin", F.when(F.col("FinalResult") == "H", 1).otherwise(0))
          .withColumn("AwayTeamWin", F.when(F.col("FinalResult") == "A", 1).otherwise(0))
          .withColumn("GameTie", F.when(F.col("FinalResult") == "D", 1).otherwise(0))
    )
    return df_indicators


def verify_result_consistency(df: DataFrame) -> None:
    """
    Vérifie la cohérence logique entre les colonnes indicatrices.

    Lève une exception si :
        - plus d’une colonne indicatrice vaut 1 sur une même ligne
        - aucune colonne indicatrice ne vaut 1

    Args:
        df (DataFrame): DataFrame contenant HomeTeamWin, AwayTeamWin, GameTie

    Raises:
        ValueError: Si des incohérences sont détectées.
    """
    df_check = (
        df.withColumn("sum_flags", F.col("HomeTeamWin") + F.col("AwayTeamWin") + F.col("GameTie"))
    )

    inconsistent = df_check.filter((F.col("sum_flags") != 1))
    nb_inconsistent = inconsistent.count()

    if nb_inconsistent > 0:
        raise ValueError(
            f"Incohérence détectée dans {nb_inconsistent} lignes : "
            f"chaque match doit avoir exactement un indicateur égal à 1."
        )


def prepare_match_indicators(df: DataFrame) -> DataFrame:
    """
    Pipeline complet de préparation des colonnes indicatrices.

    Étapes :
        1. Ajouter les colonnes HomeTeamWin, AwayTeamWin, GameTie
        2. Vérifier la cohérence des résultats

    Args:
        df (DataFrame): DataFrame brut contenant la colonne FinalResult

    Returns:
        DataFrame: DataFrame final prêt pour l’analyse.
    """
    df_with_indicators = add_result_indicators(df)
    verify_result_consistency(df_with_indicators)
    return df_with_indicators
