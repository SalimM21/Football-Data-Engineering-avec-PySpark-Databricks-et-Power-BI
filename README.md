# ⚽ Football Performance Analysis Pipeline avec PySpark, Databricks et Power BI

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

```mermaid
flowchart TB
    %% ========================
    %% 🎯 ARCHITECTURE DU PIPELINE FOOTBALL
    %% ========================

    %% === DÉFINITIONS DES STYLES ===
    classDef load fill:#D6EAF8,stroke:#2E86C1,stroke-width:2px,color:#000,font-weight:bold;        %% Bleu clair
    classDef filter fill:#D1F2EB,stroke:#117864,stroke-width:2px,color:#000,font-weight:bold;      %% Vert clair
    classDef agg fill:#FDEBD0,stroke:#CA6F1E,stroke-width:2px,color:#000,font-weight:bold;         %% Orange clair
    classDef kpi fill:#FADBD8,stroke:#C0392B,stroke-width:2px,color:#000,font-weight:bold;         %% Rouge clair
    classDef rank fill:#E8DAEF,stroke:#7D3C98,stroke-width:2px,color:#000,font-weight:bold;        %% Violet
    classDef save fill:#FCF3CF,stroke:#B7950B,stroke-width:2px,color:#000,font-weight:bold;        %% Jaune clair
    classDef vis fill:#D5F5E3,stroke:#27AE60,stroke-width:2px,color:#000,font-weight:bold;         %% Vert tendre

    %% === 1️⃣ CHARGEMENT & PRÉPARATION ===
    subgraph LOAD["📥 Chargement & préparation des données"]
        LOAD1(["📂 Import CSV dans DataFrame PySpark"])
        LOAD2(["🧹 Nettoyage & renommage des colonnes (footballcolumnsdocumentation.pdf)"])
        LOAD3(["➕ Création colonnes supplémentaires : HomeTeamWin, AwayTeamWin, GameTie"])
    end
    class LOAD1,LOAD2,LOAD3 load;

    %% === 2️⃣ FILTRAGE ===
    subgraph FILTER["🔎 Filtrage des données"]
        FILTER1(["⚽ Conserver uniquement Bundesliga (Div = D1)"])
        FILTER2(["📅 Période : 2000 à 2015"])
    end
    class FILTER1,FILTER2 filter;

    %% === 3️⃣ AGRÉGATIONS ===
    subgraph AGG["🧮 Agrégations"]
        AGG1(["🏠 df_home_matches : statistiques domicile"])
        AGG2(["🚗 df_away_matches : statistiques extérieur"])
        AGG3(["🔗 Jointure → df_merged"])
    end
    class AGG1,AGG2,AGG3 agg;

    %% === 4️⃣ KPI ===
    subgraph KPI["📊 Calcul des KPI"]
        KPI1(["⚡ Colonnes totales : GoalsScored, GoalsAgainst, Win, Loss, Tie"])
        KPI2(["📈 Colonnes avancées : GoalDifferentials, WinPercentage, GoalsPerGame, GoalsAgainstPerGame"])
    end
    class KPI1,KPI2 kpi;

    %% === 5️⃣ CLASSEMENT ===
    subgraph RANK["🏆 Classement des équipes"]
        RANK1(["📊 Window Functions : classement par WinPercentage, GoalDifferentials"])
        RANK2(["🥇 Extraction du champion : TeamPosition = 1"])
    end
    class RANK1,RANK2 rank;

    %% === 6️⃣ SAUVEGARDE ===
    subgraph SAVE["💾 Sauvegarde optimisée"]
        SAVE1(["🗂 football_stats_partitioned : toutes les équipes, partitionné par saison"])
        SAVE2(["🏅 football_top_teams : champions uniquement"])
    end
    class SAVE1,SAVE2 save;

    %% === 7️⃣ VISUALISATION ===
    subgraph VIS["📉 Visualisation & Reporting"]
        VIS1(["📊 Graphiques % victoires des champions"])
        VIS2(["⚽ Nombre de buts marqués"])
        VIS3(["📈 GoalDifferentials par saison"])
        VIS4(["🧰 Outils : Pandas / Matplotlib / Power BI"])
    end
    class VIS1,VIS2,VIS3,VIS4 vis;

    %% === 🔄 FLUX DU PIPELINE ===
    LOAD1 --> LOAD2 --> LOAD3 --> FILTER1 --> FILTER2 --> AGG1 --> AGG2 --> AGG3
    AGG3 --> KPI1 --> KPI2 --> RANK1 --> RANK2 --> SAVE1 --> SAVE2 --> VIS1
    SAVE2 --> VIS2
    SAVE2 --> VIS3
    VIS1 --> VIS4
    VIS2 --> VIS4
    VIS3 --> VIS4


```
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
football-performance-pipeline/
│
├── data/
│   ├── raw/                     # Données CSV brutes
│   │   └── football_matches.csv
│   ├── processed/               # Données transformées (parquet)
│   │   ├── football_stats_partitioned/
│   │   └── football_top_teams/
│   └── tests/                   # Jeux de données miniatures pour tests
│       └── sample_data.csv
│
├── src/
│   ├── __init__.py
│   ├── config/
│   │   └── spark_config.py      # Configuration SparkSession
│   ├── ingestion/
│   │   └── load_data.py         # Chargement CSV → DataFrame
│   ├── transformation/
│   │   ├── prepare_columns.py   # Nettoyage, renommage
│   │   ├── indicators.py        # Colonnes indicatrices (HomeTeamWin, etc.)
│   │   ├── filter_data.py       # Filtrage D1 + saisons 2000–2015
│   │   ├── aggregations.py      # GroupBy domicile / extérieur
│   │   ├── merge_data.py        # Fusion home/away
│   │   ├── metrics.py           # Colonnes synthétiques + KPIs
│   │   └── ranking.py           # Classement via Window Functions
│   ├── export/
│   │   └── save_parquet.py      # Sauvegarde des Parquet
│   ├── visualization/
│   │   └── charts.py            # Graphiques Matplotlib / Seaborn
│   └── pipeline.py              # Script principal orchestrant le pipeline
│
├── tests/
│   ├── __init__.py
│   ├── test_ingestion.py        # Tests du chargement
│   ├── test_transformation.py   # Tests transformations & indicateurs
│   ├── test_metrics.py          # Tests des calculs de KPI
│   ├── test_ranking.py          # Tests de classement
│   └── test_export.py           # Tests du format Parquet
│
├── notebooks/
│   ├── pipeline_exploration.ipynb   # Exploration initiale
│   └── debug_visuals.ipynb          # Debug + visualisation
│
├── logs/
│   └── pipeline.log              # Logs détaillés d’exécution
│
├── requirements.txt              # Dépendances (PySpark, pytest, matplotlib...)
├── README.md                     # Documentation du projet
└── .gitignore                    # Fichiers à ignorer (parquet, cache, logs)


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

### Lancer le pipeline (dans un notebook Colab)
```python
!pip install pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("FootballAnalysis").getOrCreate()
df = spark.read.csv("/content/football_matches.csv", header=True, inferSchema=True)
```
### Sauvegarde en Parquet partitionné
```python
df_final.write.mode("overwrite").partitionBy("Season").parquet("/content/football_stats_partitioned")
```

### Lecture du Parquet
```python
df = spark.read.parquet("/content/football_stats_partitioned")
df.printSchema()
```
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

Ce projet illustre la **mise en œuvre complète d’un pipeline de données PySpark**, depuis la **préparation** jusqu’à la **visualisation**, en appliquant les **bonnes pratiques d’ingénierie de données**, ainsi que les principes d’**optimisation des performances** et de **déploiement sur Databricks et Power BI**.





