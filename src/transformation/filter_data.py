# src/filter_data.py

"""
Module : filter_data.py
------------------------
Filtrage du DataFrame PySpark selon les conditions du projet.

Étapes :
1️⃣ Garder uniquement les matchs de la Bundesliga (Div = 'D1').
2️⃣ Restreindre aux saisons 2000–2015.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def filter_bundesliga_data(df: DataFrame) -> DataFrame:
    """
    Filtre les données pour ne garder que :
    - La Bundesliga (Div = 'D1')
    - Les saisons comprises entre 2000 et 2015 inclus.

    Paramètres
    ----------
    df : DataFrame
        DataFrame initial contenant les colonnes ['Div', 'Season', ...]

    Retour
    -------
    DataFrame
        DataFrame filtré
    """

    # Vérification des colonnes obligatoires
    required_cols = {"Div", "Season"}
    if not required_cols.issubset(df.columns):
        raise ValueError(f"Colonnes manquantes : {required_cols - set(df.columns)}")

    df_filtered = df.filter(
        (col("Div") == "D1") &
        (col("Season").between(2000, 2015))
    )

    return df_filtered


if __name__ == "__main__":
    from pyspark.sql import SparkSession

    # Exemple de test rapide local
    spark = SparkSession.builder.appName("FilterDataTest").getOrCreate()

    data = [
        ("D1", 2001, "TeamA", "TeamB"),
        ("D2", 2005, "TeamC", "TeamD"),
        ("D1", 2017, "TeamE", "TeamF"),
        ("D1", 2010, "TeamG", "TeamH"),
    ]
    cols = ["Div", "Season", "HomeTeam", "AwayTeam"]
    df = spark.createDataFrame(data, cols)

    df_filtered = filter_bundesliga_data(df)
    df_filtered.show()

    spark.stop()
