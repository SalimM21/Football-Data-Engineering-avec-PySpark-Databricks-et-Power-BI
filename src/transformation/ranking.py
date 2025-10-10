# src/ranking.py

"""
Module : ranking.py
-------------------
Classe les équipes par saison en utilisant les colonnes
WinPercentage et GoalDifferentials.

Colonne ajoutée :
- TeamPosition : classement de l'équipe dans la saison (1 = champion)
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def rank_teams(df: DataFrame) -> DataFrame:
    """
    Ajoute la colonne TeamPosition en fonction de WinPercentage et GoalDifferentials.

    Args:
        df (DataFrame): DataFrame avec colonnes WinPercentage et GoalDifferentials.

    Returns:
        DataFrame: DataFrame avec colonne TeamPosition ajoutée.
    """
    required_cols = {"Season", "Team", "WinPercentage", "GoalDifferentials"}
    missing_cols = required_cols - set(df.columns)
    if missing_cols:
        raise ValueError(f"Colonnes manquantes dans df : {missing_cols}")

    # Définition de la fenêtre par saison
    window_spec = Window.partitionBy("Season").orderBy(
        F.desc("WinPercentage"),
        F.desc("GoalDifferentials")
    )

    df_ranked = df.withColumn("TeamPosition", F.row_number().over(window_spec))
    return df_ranked


if __name__ == "__main__":
    from pyspark.sql import SparkSession
    from compute_synthetic import compute_synthetic_stats

    spark = SparkSession.builder.appName("RankingTest").getOrCreate()

    # Exemple minimal
    data = [
        (2010, "Bayern", 0.75, 5),
        (2010, "Dortmund", 0.50, 2),
        (2010, "Leverkusen", 0.50, 3)
    ]
    columns = ["Season", "Team", "WinPercentage", "GoalDifferentials"]

    df = spark.createDataFrame(data, columns)

    df_ranked = rank_teams(df)
    df_ranked.show()

    spark.stop()
