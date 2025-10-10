from src.config.spark_config import create_spark
from src.ingestion.load_data import load_csv
from src.transformation.prepare_columns import rename_columns
from src.transformation.indicators import add_indicators
from src.transformation.filter_data import filter_bundesliga
from src.transformation.aggregations import compute_home_away_stats
from src.transformation.merge_data import merge_stats
from src.transformation.metrics import compute_metrics
from src.transformation.ranking import add_team_ranking
from src.export.save_parquet import save_datasets

def main():
    spark = create_spark("FootballPerformancePipeline")
    df = load_csv(spark, "data/raw/football_matches.csv")
    df = rename_columns(df)
    df = add_indicators(df)
    df = filter_bundesliga(df)
    df_home, df_away = compute_home_away_stats(df)
    df_merged = merge_stats(df_home, df_away)
    df_processed = compute_metrics(df_merged)
    df_ranked = add_team_ranking(df_processed)
    save_datasets(df_ranked)

if __name__ == "__main__":
    main()
