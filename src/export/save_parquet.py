# src/save_parquet.py

"""
Module : save_parquet.py
------------------------
Filtre les champions et sauvegarde les datasets PySpark au format Parquet.
- football_stats_partitioned : toutes les équipes, partitionné par saison
- football_top_teams : champions uniquement
"""

from pyspark.sql import DataFrame


def save_partitioned_parquet(df: DataFrame, output_path: str, partition_col: str = "Season"):
    """
    Sauvegarde le DataFrame au format Parquet avec partitionnement par saison.

    Args:
        df (DataFrame): DataFrame à sauvegarder
        output_path (str): chemin du dossier de sortie
        partition_col (str): colonne de partitionnement (par défaut "Season")
    """
    df.write.mode("overwrite").partitionBy(partition_col).parquet(output_path)


def extract_champions(df_ranked: DataFrame) -> DataFrame:
    """
    Filtre les équipes championnes (TeamPosition = 1).

    Args:
        df_ranked (DataFrame): DataFrame contenant la colonne TeamPosition

    Returns:
        DataFrame: DataFrame contenant uniquement les champions
    """
    if "TeamPosition" not in df_ranked.columns:
        raise ValueError("La colonne 'TeamPosition' est manquante dans df_ranked")

    champions_df = df_ranked.filter(df_ranked.TeamPosition == 1)
    return champions_df


if __name__ == "__main__":
    from pyspark.sql import SparkSession
    from ranking import rank_teams
    from compute_synthetic import compute_synthetic_stats

    spark = SparkSession.builder.appName("SaveParquetTest").getOrCreate()

    # Exemple minimal
    data = [
        (2010, "Bayern", 1, 3, 0, 0, 2, 3, 1, 0),
        (2010, "Dortmund", 0, 1, 1, 1, 2, 0, 0, 0)
    ]
    columns = [
        "Season", "Team",
        "TotalHomeWin", "HomeScoredGoals", "TotalHomeLoss",
        "TotalAwayWin", "AwayScoredGoals", "TotalAwayLoss",
        "TotalHomeTie", "TotalAwayTie"
    ]

    df = spark.createDataFrame(data, columns)
    df_synthetic = compute_synthetic_stats(df)
    df_ranked = rank_teams(df_synthetic)

    # Sauvegarde toutes les équipes partitionnées par saison
    save_partitioned_parquet(df_ranked, "football_stats_partitioned")

    # Extraction et sauvegarde des champions
    champions_df = extract_champions(df_ranked)
    champions_df.write.mode("overwrite").parquet("football_top_teams")

    spark.stop()
