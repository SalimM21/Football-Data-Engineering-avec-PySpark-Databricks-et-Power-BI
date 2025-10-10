# src/merge_data.py

"""
Module : merge_data.py
----------------------
Fusionne les DataFrames d'agrégations à domicile et à l'extérieur
pour obtenir un DataFrame complet par équipe et par saison.
"""

from pyspark.sql import DataFrame


def merge_home_away(df_home: DataFrame, df_away: DataFrame) -> DataFrame:
    """
    Effectue la jointure entre les statistiques à domicile et à l'extérieur.

    Args:
        df_home (DataFrame): DataFrame des stats domicile (colonnes Season, Team, TotalHomeWin, ...)
        df_away (DataFrame): DataFrame des stats extérieur (colonnes Season, Team, TotalAwayWin, ...)

    Returns:
        DataFrame: DataFrame fusionné avec toutes les statistiques par Team et Season.
    """

    # Vérification des colonnes requises
    required_cols_home = {"Season", "Team"}
    required_cols_away = {"Season", "Team"}

    if not required_cols_home.issubset(df_home.columns):
        raise ValueError(f"Colonnes manquantes dans df_home : {required_cols_home - set(df_home.columns)}")
    if not required_cols_away.issubset(df_away.columns):
        raise ValueError(f"Colonnes manquantes dans df_away : {required_cols_away - set(df_away.columns)}")

    df_merged = df_home.join(df_away, on=["Season", "Team"], how="outer")
    return df_merged


if __name__ == "__main__":
    from pyspark.sql import SparkSession
    from aggregations import aggregate_home_stats, aggregate_away_stats
    from filter_data import filter_bundesliga_data
    from prepare_columns import prepare_columns
    from indicators import add_match_indicators

    spark = SparkSession.builder.appName("MergeDataTest").getOrCreate()

    # Exemple minimal
    data = [
        (2010, "Bayern", "Dortmund", 3, 1),
        (2010, "Dortmund", "Bayern", 1, 2)
    ]
    columns = ["Season", "HomeTeam", "AwayTeam", "FTHG", "FTAG"]
    df = spark.createDataFrame(data, columns)

    # Préparation des colonnes et indicateurs
    df_prepared = prepare_columns(df)
    df_indicators = add_match_indicators(df_prepared)
    df_filtered = filter_bundesliga_data(df_indicators)

    # Agrégations
    df_home = aggregate_home_stats(df_filtered)
    df_away = aggregate_away_stats(df_filtered)

    # Fusion
    df_merged = merge_home_away(df_home, df_away)
    df_merged.show()

    spark.stop()
