# tests/test_save_parquet.py

"""
Tests unitaires pour le module save_parquet.py
-----------------------------------------------
Vérifie extract_champions() et save_partitioned_parquet()
"""

import pytest
import os
import shutil
from pyspark.sql import SparkSession, Row
from save_parquet import extract_champions, save_partitioned_parquet


@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder \
        .appName("TestSaveParquet") \
        .master("local[*]") \
        .getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture
def sample_ranked_data(spark):
    data = [
        Row(Season=2010, Team="Bayern", TeamPosition=1),
        Row(Season=2010, Team="Dortmund", TeamPosition=2),
        Row(Season=2011, Team="Bayern", TeamPosition=2),
        Row(Season=2011, Team="Dortmund", TeamPosition=1)
    ]
    return spark.createDataFrame(data)


def test_extract_champions(sample_ranked_data):
    """Vérifie que les champions sont correctement filtrés"""
    champions_df = extract_champions(sample_ranked_data)
    result = [row.Team for row in champions_df.collect()]
    assert set(result) == {"Bayern", "Dortmund"}
    assert all(row.TeamPosition == 1 for row in champions_df.collect())


def test_extract_champions_missing_column(spark):
    """Vérifie que l'erreur est levée si TeamPosition est manquante"""
    df_invalid = spark.createDataFrame([
        (2010, "Bayern")
    ], ["Season", "Team"])
    with pytest.raises(ValueError, match="La colonne 'TeamPosition' est manquante"):
        extract_champions(df_invalid)


def test_save_partitioned_parquet(sample_ranked_data, tmp_path):
    """Vérifie que le DataFrame est sauvegardé partitionné par saison"""
    output_dir = tmp_path / "test_parquet"
    save_partitioned_parquet(sample_ranked_data, str(output_dir), partition_col="Season")

    # Vérifie que des sous-dossiers par saison ont été créés
    seasons = [d.name for d in os.scandir(output_dir) if d.is_dir()]
    assert set(seasons) == {"Season=2010", "Season=2011"}

    # Nettoyage
    shutil.rmtree(output_dir)
