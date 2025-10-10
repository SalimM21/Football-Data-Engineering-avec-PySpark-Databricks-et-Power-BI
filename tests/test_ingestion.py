# tests/test_load_data.py
"""
Tests unitaires pour le module load_data.py
-------------------------------------------
Ces tests valident le bon fonctionnement des fonctions de chargement et de préparation
des données footballistiques avec PySpark.

🔍 Ce que les tests vérifient :
    1. Le CSV est bien chargé et contient les bonnes colonnes
    2. Les colonnes sont correctement renommées
    3. Les colonnes inutiles ont été supprimées
"""

import pytest
from pyspark.sql import SparkSession
from src.load_data import load_csv_data, clean_and_rename_columns

# -------------------------------------------------------------------------
# 🧰 FIXTURE : Crée une session Spark de test
# -------------------------------------------------------------------------
@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .appName("FootballDataTest")
        .master("local[*]")
        .getOrCreate()
    )
    yield spark
    spark.stop()

# -------------------------------------------------------------------------
# 📘 FIXTURE : Crée un DataFrame PySpark d’exemple (simule le CSV)
# -------------------------------------------------------------------------
@pytest.fixture
def sample_df(spark):
    data = [
        (1, "D1", 2010, "2010-08-15", "Bayern", "Dortmund", 2, 1, "H"),
        (2, "D1", 2010, "2010-08-22", "Leverkusen", "Bayern", 0, 3, "A"),
        (3, "E0", 2010, "2010-09-10", "Chelsea", "Arsenal", 2, 2, "D"),
    ]
    columns = ["Match_ID", "Div", "Season", "Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG", "FTR"]
    return spark.createDataFrame(data, columns)

# -------------------------------------------------------------------------
# ✅ TEST 1 : Vérifier que le DataFrame est bien chargé
# -------------------------------------------------------------------------
def test_load_csv_data(spark, tmp_path):
    # Création d’un petit CSV temporaire
    csv_file = tmp_path / "test_data.csv"
    with open(csv_file, "w") as f:
        f.write("Match_ID,Div,Season,Date,HomeTeam,AwayTeam,FTHG,FTAG,FTR\n")
        f.write("1,D1,2010,2010-08-15,Bayern,Dortmund,2,1,H\n")

    df = load_csv_data(spark, str(csv_file))
    assert df.count() == 1
    assert "HomeTeam" in df.columns

# -------------------------------------------------------------------------
# ✅ TEST 2 : Vérifier le renommage et nettoyage des colonnes
# -------------------------------------------------------------------------
def test_clean_and_rename_columns(sample_df):
    df_cleaned = clean_and_rename_columns(sample_df)

    expected_cols = [
        "Match_ID", "Div", "Season", "Date",
        "HomeTeam", "AwayTeam", "HomeTeamGoals", "AwayTeamGoals", "FinalResult"
    ]

    # Vérifier que toutes les colonnes attendues sont présentes
    assert all(col in df_cleaned.columns for col in expected_cols)

    # Vérifier que les anciennes colonnes ont disparu
    assert "FTHG" not in df_cleaned.columns
    assert "FTAG" not in df_cleaned.columns
    assert "FTR" not in df_cleaned.columns

    # Vérifier que les valeurs ont été bien renommées
    first_row = df_cleaned.collect()[0]
    assert isinstance(first_row.HomeTeamGoals, int)
    assert first_row.FinalResult in ["H", "A", "D"]

# -------------------------------------------------------------------------
# ✅ TEST 3 : Vérifier le nombre de colonnes finales
# -------------------------------------------------------------------------
def test_number_of_columns_after_cleaning(sample_df):
    df_cleaned = clean_and_rename_columns(sample_df)
    assert len(df_cleaned.columns) == 9  # Colonnes finales
