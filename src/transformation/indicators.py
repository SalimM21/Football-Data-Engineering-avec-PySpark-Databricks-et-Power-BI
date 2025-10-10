# src/indicators.py

"""
Module : indicators.py
-----------------------
Ce module crée les colonnes indicatrices pour les résultats des matchs :
- HomeTeamWin : 1 si l'équipe à domicile gagne, sinon 0
- AwayTeamWin : 1 si l'équipe à l'extérieur gagne, sinon 0
- GameTie : 1 si match nul, sinon 0
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_match_indicators(df: DataFrame) -> DataFrame:
    """
    Ajoute les colonnes indicatrices des résultats de match :
    - HomeTeamWin
    - AwayTeamWin
    - GameTie

    Args:
        df (DataFrame): DataFrame d'entrée contenant au minimum les colonnes 'FTHG' (buts domicile)
                        et 'FTAG' (buts extérieur).

    Returns:
        DataFrame: Nouveau DataFrame avec les colonnes indicatrices ajoutées.
    """
    df_indicators = (
        df.withColumn("HomeTeamWin", F.when(F.col("FTHG") > F.col("FTAG"), F.lit(1)).otherwise(F.lit(0)))
          .withColumn("AwayTeamWin", F.when(F.col("FTHG") < F.col("FTAG"), F.lit(1)).otherwise(F.lit(0)))
          .withColumn("GameTie", F.when(F.col("FTHG") == F.col("FTAG"), F.lit(1)).otherwise(F.lit(0)))
    )
    return df_indicators


if __name__ == "__main__":
    from spark_config import get_spark_session
    spark = get_spark_session("IndicatorsTest")

    # Exemple d’utilisation
    data = [
        ("TeamA", "TeamB", 2, 1),
        ("TeamC", "TeamD", 0, 3),
        ("TeamE", "TeamF", 2, 2),
    ]
    columns = ["HomeTeam", "AwayTeam", "FTHG", "FTAG"]

    df = spark.createDataFrame(data, columns)
    df_result = add_match_indicators(df)
    df_result.show()
