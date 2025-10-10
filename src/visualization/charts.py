# src/visualization.py

"""
Module : visualization.py
-------------------------
Visualisation des performances des champions de football par saison.

Graphiques générés :
1. % de victoires des champions
2. Nombre de buts marqués
3. GoalDifferentials
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

sns.set_style("whitegrid")


def plot_win_percentage(df: pd.DataFrame):
    """
    Graphique : % de victoires des champions par saison
    """
    plt.figure(figsize=(10, 6))
    sns.barplot(x="Season", y="WinPercentage", data=df, palette="viridis")
    plt.title("Pourcentage de victoires des champions par saison")
    plt.ylabel("Win Percentage")
    plt.xlabel("Saison")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()


def plot_goals_scored(df: pd.DataFrame):
    """
    Graphique : nombre de buts marqués par saison
    """
    plt.figure(figsize=(10, 6))
    sns.barplot(x="Season", y="GoalsScored", data=df, palette="rocket")
    plt.title("Buts marqués par les champions par saison")
    plt.ylabel("Buts marqués")
    plt.xlabel("Saison")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()


def plot_goal_differentials(df: pd.DataFrame):
    """
    Graphique : GoalDifferentials des champions par saison
    """
    plt.figure(figsize=(10, 6))
    sns.barplot(x="Season", y="GoalDifferentials", data=df, palette="mako")
    plt.title("Différence de buts des champions par saison")
    plt.ylabel("Goal Differential")
    plt.xlabel("Saison")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    # Exemple avec Pandas
    data = {
        "Season": ["2010", "2011", "2012"],
        "Team": ["Bayern", "Dortmund", "Bayern"],
        "WinPercentage": [0.75, 0.80, 0.70],
        "GoalsScored": [80, 75, 78],
        "GoalDifferentials": [50, 45, 40]
    }
    df_champions = pd.DataFrame(data)

    plot_win_percentage(df_champions)
    plot_goals_scored(df_champions)
    plot_goal_differentials(df_champions)
