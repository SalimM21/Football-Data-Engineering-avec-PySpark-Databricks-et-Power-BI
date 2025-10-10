# src/load_data.py
"""
Module : Chargement et préparation initiale des données
-------------------------------------------------------
Ce script charge le fichier CSV des matchs de football, comprend les colonnes,
renomme celles utiles et supprime celles inutiles.

Étapes :
    1. Charger le CSV dans un DataFrame PySpark
    2. Afficher la signification des colonnes (documentation)
    3. Renommer et supprimer les colonnes inutiles
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def load_csv_data(spark: SparkSession, csv_path: str):
    """
    Charge un fichier CSV dans un DataFrame PySpark avec gestion automatique du schéma.

    Args:
        spark (SparkSession): La session Spark active.
        csv_path (str): Chemin du fichier CSV (dans Google Colab ou Databricks).

    Returns:
        DataFrame: DataFrame PySpark contenant les données chargées.
    """
    print("📂 Chargement du fichier CSV en cours...")

    df_raw = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(csv_path)
    )

    print(f"✅ Données chargées avec {df_raw.count()} lignes et {len(df_raw.columns)} colonnes.")
    return df_raw


def describe_columns():
    """
    Affiche la signification des colonnes du CSV brut.
    """
    print("\n📘 Description des colonnes brutes :")
    print("""
    - Match_ID  : Identifiant unique du match
    - Div        : Division ou niveau du championnat (ex : D1 = Bundesliga)
    - Season     : Année de début de la saison (ex : 2005)
    - Date       : Date du match (AAAA-MM-JJ)
    - HomeTeam   : Équipe à domicile
    - AwayTeam   : Équipe à l’extérieur
    - FTHG       : Buts marqués par l’équipe à domicile
    - FTAG       : Buts marqués par l’équipe à l’extérieur
    - FTR        : Résultat final du match (H = Home win, A = Away win, D = Draw)
    """)


def clean_and_rename_columns(df_raw):
    """
    Renomme les colonnes clés et supprime celles inutiles.

    Args:
        df_raw (DataFrame): DataFrame brut issu du CSV.

    Returns:
        DataFrame: DataFrame nettoyé et renommé.
    """
    print("\n🧹 Nettoyage et renommage des colonnes...")

    df_cleaned = (
        df_raw
        .withColumnRenamed("FTHG", "HomeTeamGoals")
        .withColumnRenamed("FTAG", "AwayTeamGoals")
        .withColumnRenamed("FTR", "FinalResult")
        .select("Match_ID", "Div", "Season", "Date",
                "HomeTeam", "AwayTeam", "HomeTeamGoals", "AwayTeamGoals", "FinalResult")
    )

    print(f"✅ Colonnes finales : {df_cleaned.columns}")
    return df_cleaned


def load_and_prepare_data(spark: SparkSession, csv_path: str):
    """
    Pipeline complet : chargement + description + nettoyage.

    Args:
        spark (SparkSession): Session Spark.
        csv_path (str): Chemin vers le CSV brut.

    Returns:
        DataFrame: DataFrame nettoyé prêt pour les étapes suivantes.
    """
    df_raw = load_csv_data(spark, csv_path)
    describe_columns()
    df_cleaned = clean_and_rename_columns(df_raw)
    return df_cleaned


if __name__ == "__main__":
    # Exemple d'exécution locale (Google Colab)
    spark = (
        SparkSession.builder
        .appName("FootballDataLoad")
        .master("local[*]")
        .getOrCreate()
    )

    csv_path = "/content/football_data.csv"  # 📌 à adapter selon ton chemin
    df_prepared = load_and_prepare_data(spark, csv_path)

    print("\n📊 Aperçu des données préparées :")
    df_prepared.show(5)
