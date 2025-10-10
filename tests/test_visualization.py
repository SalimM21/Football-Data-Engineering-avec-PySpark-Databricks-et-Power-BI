# tests/test_visualization.py

"""
Tests unitaires pour le module visualization.py
-----------------------------------------------
Vérifie que les fonctions de visualisation fonctionnent correctement
avec un DataFrame Pandas simulé.
"""

import pytest
import pandas as pd
from visualization import plot_win_percentage, plot_goals_scored, plot_goal_differentials


@pytest.fixture
def sample_champions_df():
    """Crée un DataFrame Pandas simulant les champions par saison"""
    data = {
        "Season": ["2010", "2011", "2012"],
        "Team": ["Bayern", "Dortmund", "Bayern"],
        "WinPercentage": [0.75, 0.80, 0.70],
        "GoalsScored": [80, 75, 78],
        "GoalDifferentials": [50, 45, 40]
    }
    return pd.DataFrame(data)


def test_plot_win_percentage(sample_champions_df):
    """Test que plot_win_percentage s'exécute sans erreur"""
    try:
        plot_win_percentage(sample_champions_df)
    except Exception as e:
        pytest.fail(f"plot_win_percentage a échoué : {e}")


def test_plot_goals_scored(sample_champions_df):
    """Test que plot_goals_scored s'exécute sans erreur"""
    try:
        plot_goals_scored(sample_champions_df)
    except Exception as e:
        pytest.fail(f"plot_goals_scored a échoué : {e}")


def test_plot_goal_differentials(sample_champions_df):
    """Test que plot_goal_differentials s'exécute sans erreur"""
    try:
        plot_goal_differentials(sample_champions_df)
    except Exception as e:
        pytest.fail(f"plot_goal_differentials a échoué : {e}")
