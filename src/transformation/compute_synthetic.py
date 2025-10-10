# src/compute_synthetic.py

"""
Module : compute_synthetic.py
-----------------------------
Calcul des colonnes synthétiques et avancées pour les équipes
après fusion des statistiques domicile et extérieur.

Colonnes totales :
- GoalsScored, GoalsAgainst, Win, Loss, Tie

Colonnes avancées :
- GoalDifferentials, WinPercentage, GoalsPerGame, GoalsAgainstPerGame
- TotalGames
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def compute_synthetic_stats(df: DataFrame) -> DataFrame:
    """
    Ajoute les colonnes synthétiques et avancées au DataFrame fusionné.

    Args:
        df (DataFrame): DataFrame fusionné avec colonnes domicile et extérieur.

    Returns:
        DataFrame: DataFrame enrichi avec les colonnes totales et avancées.
    """

    # Colonnes totales
    df = df.withColumn(
        "GoalsScored",
        F.coalesce(F.col("HomeScoredGoals"), F.lit(0)) + F.coalesce(F.col("AwayScoredGoals"), F.lit(0))
    ).withColumn(
        "GoalsAgainst",
        F.coalesce(F.col("HomeAgainstGoals"), F.lit(0)) + F.coalesce(F.col("AwayAgainstGoals"), F.lit(0))
    ).withColumn(
        "Win",
        F.coalesce(F.col("TotalHomeWin"), F.lit(0)) + F.coalesce(F.col("TotalAwayWin"), F.lit(0))
    ).withColumn(
        "Loss",
        F.coalesce(F.col("TotalHomeLoss"), F.lit(0)) + F.coalesce(F.col("TotalAwayLoss"), F.lit(0))
    ).withColumn(
        "Tie",
        F.coalesce(F.col("TotalHomeTie"), F.lit(0)) + F.coalesce(F.col("TotalAwayTie"), F.lit(0))
    )

    # Total de matchs joués
    df = df.withColumn("TotalGames", F.col("Win") + F.col("Loss") + F.col("Tie"))

    # Colonnes avancées
    df = df.withColumn("GoalDifferentials", F.col("GoalsScored") - F.col("GoalsAgainst"))
    df = df.withColumn("WinPercentage", F.when(F.col("TotalGames") > 0,
                                               F.col("Win") / F.col("TotalGames")).otherwise(0))
    df = df.withColumn("GoalsPerGame", F.when(F.col("TotalGames") > 0,
                                              F.col("GoalsScored") / F.col("TotalGames")).otherwise(0))
    df = df.withColumn("GoalsAgainstPerGame", F.when(F.col("TotalGames") > 0,
                                                     F.col("GoalsAgainst") / F.col("TotalGames")).otherwise(0))

    return df


if __name__ == "__main__":
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("ComputeSyntheticTest").getOrCreate()

    data = [
        (2010, "Bayern", 1, 3, 0, 1, 0, 2, 1, 0),
        (2010, "Dortmund", 0, 1, 1, 0, 0, 1, 2, 0)
    ]
    columns = [
        "Season", "Team",
        "TotalHomeWin", "HomeScoredGoals", "TotalHomeLoss",
        "TotalAwayWin", "AwayScoredGoals", "TotalAwayLoss",
        "TotalHomeTie", "TotalAwayTie"
    ]
    df = spark.createDataFrame(data, columns)

    df_synthetic = compute_synthetic_stats(df)
    df_synthetic.show()

    spark.stop()
