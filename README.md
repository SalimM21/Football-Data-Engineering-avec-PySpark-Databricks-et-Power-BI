# ⚽ Football Performance Analysis Pipeline (PySpark)

## 📖 Contexte du projet

Les données footballistiques représentent une source précieuse pour analyser les performances des équipes, identifier les tendances saisonnières et soutenir la prise de décision dans le sport.  
Ce projet a pour objectif de construire un **pipeline PySpark complet** permettant d’analyser la performance des équipes **saison par saison**, de calculer des **indicateurs clés de performance (KPI)**, de **classer les équipes**, et de **stocker les résultats au format Parquet partitionné**.

---

## 🎯 Objectifs

- Charger et préparer les données de matchs de football.
- Calculer des statistiques avancées par équipe et par saison.
- Identifier les **champions de chaque saison**.
- Sauvegarder les résultats sous format **Parquet partitionné par saison**.
- Visualiser les performances des équipes gagnantes.
- (Bonus) Intégrer le pipeline dans **Databricks** et créer un **dashboard Power BI**.

---

## 👥 User Stories

| Rôle | Besoin | Résultat attendu |
|------|---------|------------------|
| **Analyste** | Visualiser un tableau clair des performances par équipe et par saison. | Table des KPI par saison. |
| **Supporter** | Connaître les champions de chaque saison. | Tableau ou graphique des équipes championnes. |
| **Manager sportif** | Disposer d’indicateurs (% victoires, buts marqués, goal diff) pour orienter la stratégie. | Dashboard Power BI ou graphiques. |

---

## ⚙️ Architecture du pipeline

### Étapes principales :

1. **Chargement et préparation des données**
   - Import du fichier CSV dans un DataFrame PySpark.
   - Nettoyage et renommage des colonnes selon `footballcolumnsdocumentation.pdf`.

2. **Création de colonnes supplémentaires**
   - Colonnes indicatrices :  
     `HomeTeamWin`, `AwayTeamWin`, `GameTie`.

3. **Filtrage des données**
   - Conservation uniquement de la **Bundesliga (Div = D1)**.  
   - Période : **2000 à 2015**.

4. **Agrégations**
   - `df_home_matches` : statistiques à domicile.  
   - `df_away_matches` : statistiques à l’extérieur.  
   - Jointure → `df_merged`.

5. **Calcul des KPI**
   - Colonnes totales : `GoalsScored`, `GoalsAgainst`, `Win`, `Loss`, `Tie`.  
   - Colonnes avancées :  
     `GoalDifferentials`, `WinPercentage`, `GoalsPerGame`, `GoalsAgainstPerGame`.

6. **Classement des équipes**
   - Utilisation des **Window Functions** pour classer les équipes selon :
     - `WinPercentage`
     - `GoalDifferentials`
   - Extraction du champion : `TeamPosition = 1`.

7. **Sauvegarde optimisée**
   - `football_stats_partitioned` : toutes les équipes, partitionné par saison.  
   - `football_top_teams` : champions uniquement.

8. **Visualisation (Pandas / Matplotlib / Power BI)**
   - Graphiques :
     - % de victoires des champions.
     - Nombre de buts marqués.
     - GoalDifferentials par saison.

---

## 🧩 Structure du projet

```
football-pyspark-pipeline/
│
├── notebooks/
│ └── football_analysis_pyspark.ipynb # Notebook Google Colab ou Databricks
│
├── src/
│ └── pipeline_utils.py # Fonctions PySpark (agrégations, KPIs, ranking)
│
├── data/
│ ├── football_matches.csv # Données sources
│ ├── football_stats_partitioned/ # Parquet partitionné par saison
│ └── football_top_teams/ # Champions
│
├── visuals/
│ ├── win_percentage_champions.png
│ ├── goals_scored_champions.png
│ └── goal_differentials_champions.png
│
├── footballcolumnsdocumentation.pdf # Documentation colonnes
└── README.md

```

---

## 📊 Exemples de KPI calculés

| KPI | Description | Formule |
|-----|--------------|----------|
| **WinPercentage** | Pourcentage de victoires | `Wins / (Wins + Losses + Ties)` |
| **GoalsPerGame** | Moyenne de buts marqués par match | `GoalsScored / GamesPlayed` |
| **GoalsAgainstPerGame** | Moyenne de buts encaissés par match | `GoalsAgainst / GamesPlayed` |
| **GoalDifferentials** | Différence de buts | `GoalsScored - GoalsAgainst` |

---

## 🪶 Technologies utilisées

| Catégorie | Outils |
|------------|--------|
| **Langage principal** | Python 3.10+ |
| **Traitement distribué** | Apache Spark / PySpark |
| **Stockage optimisé** | Parquet partitionné |
| **Exploration / Visualisation** | Pandas, Matplotlib, Power BI |
| **Environnement d’exécution** | Google Colab, Databricks Community Edition |
| **Gestion de version** | GitHub |

---

## 🧠 Concepts techniques clés

- **PySpark DataFrame API** : pour les transformations distribuées.
- **GroupBy & Aggregations** : statistiques par équipe et saison.
- **Window Functions** : classement par saison.
- **Parquet partitionné** : stockage optimisé pour la lecture sélective.
- **Matplotlib / Power BI** : visualisation de la performance.

---

## 🧱 Commandes utiles

### ▶️ Lancer le pipeline (dans un notebook Colab)
```python
!pip install pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("FootballAnalysis").getOrCreate()
df = spark.read.csv("/content/football_matches.csv", header=True, inferSchema=True)
```
### 💾 Sauvegarde en Parquet partitionné
```python
df_final.write.mode("overwrite").partitionBy("Season").parquet("/content/football_stats_partitioned")
```

### 📈 Lecture du Parquet
```python
df = spark.read.parquet("/content/football_stats_partitioned")
df.printSchema()
```
# ⚽ Football Data Engineering avec PySpark, Databricks et Power BI

## 🏆 Résultats attendus

### 📂 Datasets générés

- **`football_stats_partitioned`** : contient toutes les équipes, partitionné par **saison**.  
- **`football_top_teams`** : contient uniquement les **champions** de chaque saison.

### 📊 Visualisations attendues

1. **Pourcentage de victoires des champions**
2. **Nombre de buts marqués par saison**
3. **GoalDifferentials (différence de buts)** par saison

---

## Bonus : Déploiement Databricks et Power BI

### 💻 Sur Databricks
- Recréer le pipeline dans un **notebook Databricks Community Edition (CE)**.  
- Comparer les **performances** entre **Google Colab** et **Databricks**.  
- Utiliser la commande :
  ```python
  display(df)
  ```
### 📈 Sur Power BI

- Importer les fichiers **`.parquet`** générés depuis le dossier : ``/data``

- Créer un **tableau de bord interactif** comprenant :
- Pourcentage de victoires (**%Win**)
- Nombre de buts marqués (**Goals**)
- Différence de buts (**GoalDiff**)

- Ajouter des **filtres dynamiques** :
- Par **saison**
- Par **équipe**

---

## 📚 Références

- [Apache Spark — Documentation officielle](https://spark.apache.org/docs/latest/)
- [Databricks Community Edition](https://community.cloud.databricks.com/)
- [Power BI — Importer des fichiers Parquet](https://learn.microsoft.com/fr-fr/power-bi/connect-data/desktop-connect-parquet)
- [Football Data Source — Kaggle](https://www.kaggle.com/datasets)
- [Football Data Europe](https://www.football-data.co.uk/)

---

## 🧑‍💻 Auteur

**Salim Majide**  
🎓 *Ingénieur Big Data & Cloud Computing (ENSET Mohammedia)*  
💼 *Projet Data Engineering - PySpark*  
📧 **Contact : [LinkedIn](https://www.linkedin.com/in/salim-majide-231319172)** 

---

## 📦 Livrables

1. **Notebook Google Colab ou Databricks commenté**  
2. **Datasets Parquet :**
``/data/football_stats_partitioned``
``/data/football_top_teams``
3. **Graphiques :** Win%, Goals, GoalDiff  
4. **Lien GitHub :** Notebook, Datasets Parquet, Visualisations, README

---

## 🏁 Conclusion

Ce projet illustre la **mise en œuvre complète d’un pipeline de données PySpark**,  
depuis la **préparation** jusqu’à la **visualisation**, en appliquant les **bonnes pratiques d’ingénierie de données**,  
ainsi que les principes d’**optimisation des performances** et de **déploiement sur Databricks et Power BI**.





