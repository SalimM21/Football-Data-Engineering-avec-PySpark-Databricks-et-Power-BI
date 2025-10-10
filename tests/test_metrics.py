import pytest
from src.transformation.metrics import compute_metrics

def test_goal_differentials(spark_session, sample_df):
    result_df = compute_metrics(sample_df)
    row = result_df.collect()[0]
    assert row.GoalDifferentials == row.GoalsScored - row.GoalsAgainst
