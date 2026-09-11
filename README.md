# 🚀 TradeCorp Data Platform

## 📋 Présentation

**TradeCorp Data Platform** est un projet de Data Engineering visant à automatiser le traitement des données commerciales de l'entreprise.

TradeCorp reçoit régulièrement des fichiers CSV contenant des données commerciales : clients, commandes, produits, employés, fournisseurs, catégories, transporteurs et détails des commandes.

Actuellement, une partie du traitement est réalisée manuellement, ce qui entraîne :

* un temps de traitement important ;
* des risques d'erreurs humaines ;
* des difficultés pour traiter des volumes de données importants ;
* un manque d'automatisation ;
* des difficultés de suivi et de maintenance.

L'objectif du projet est donc de mettre en place un **pipeline de données automatisé, sécurisé et évolutif**.

---

# 🎯 Objectifs du projet

Le projet a pour objectifs de :

* centraliser les données commerciales ;
* automatiser l'ingestion des fichiers CSV ;
* stocker les données dans le cloud ;
* nettoyer et contrôler la qualité des données ;
* enrichir les données avec des sources externes ;
* transformer les données avec Apache Spark ;
* automatiser les différentes étapes avec Apache Airflow ;
* sécuriser les informations sensibles ;
* améliorer les performances du traitement ;
* préparer l'architecture à une augmentation du volume de données.

---

# 🏗️ Architecture du projet

L'architecture globale est basée sur plusieurs composants :

```text
                    ┌─────────────────────┐
                    │     Fichiers CSV    │
                    │   Données brutes    │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │     Azure ADLS Gen2 │
                    │     Stockage RAW    │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │       Airflow       │
                    │   Orchestration     │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │    Apache Spark     │
                    │      PySpark        │
                    └──────────┬──────────┘
                               │
              ┌────────────────┼────────────────┐
              ▼                ▼                ▼
        Nettoyage         Enrichissement    Transformation
              │                │                │
              └────────────────┼────────────────┘
                               ▼
                    ┌─────────────────────┐
                    │   Données propres   │
                    │   et transformées   │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │       Analyse       │
                    │    / exploitation   │
                    └─────────────────────┘
```

---

# 🛠️ Technologies utilisées

| Technologie                | Utilisation                             |
| -------------------------- | --------------------------------------- |
| **Python**                 | Développement des traitements           |
| **PySpark / Apache Spark** | Nettoyage et transformation des données |
| **Apache Airflow**         | Orchestration du pipeline               |
| **Azure ADLS Gen2**        | Stockage des données                    |
| **Azure Key Vault**        | Gestion sécurisée des secrets           |
| **Docker**                 | Conteneurisation des services           |
| **PostgreSQL**             | Base de données                         |
| **Jupyter Notebook**       | Exploration et analyse des données      |
| **Git / GitHub**           | Versionnement du code                   |
| **Notion**                 | Gestion et suivi du projet              |

---

# 📂 Sources de données

Les données commerciales sont fournies sous forme de fichiers CSV.

Les principales sources sont :

```text
data/
├── categories.csv
├── customers.csv
├── employees.csv
├── order_details.csv
├── orders.csv
├── products.csv
├── shippers.csv
└── suppliers.csv
```

### Principales données

* **customers** : informations sur les clients ;
* **orders** : commandes ;
* **order_details** : détails des commandes ;
* **products** : produits ;
* **categories** : catégories de produits ;
* **suppliers** : fournisseurs ;
* **employees** : employés ;
* **shippers** : transporteurs.

---

# 🔄 Pipeline de données

Le pipeline suit plusieurs étapes.

## 1. Ingestion

Les fichiers CSV sont récupérés et déposés dans la zone de stockage **Raw**.

```text
CSV → Azure ADLS Gen2 / Raw
```

L'objectif est de conserver les données originales avant transformation.

---

## 2. Lecture des données

Les fichiers sont ensuite chargés avec **PySpark**.

Exemple :

```python
df_orders = spark.read.csv(
    "/home/jovyan/data/orders.csv",
    header=True,
    inferSchema=True
)
```

Le schéma des données est ensuite vérifié :

```python
df_orders.printSchema()
```

---

## 3. Nettoyage

Les données sont contrôlées et nettoyées avant leur utilisation.

Les traitements peuvent inclure :

* suppression des espaces inutiles ;
* gestion des valeurs nulles ;
* suppression des doublons ;
* correction des types ;
* contrôle des dates ;
* contrôle des valeurs aberrantes ;
* validation des identifiants ;
* standardisation des chaînes de caractères.

Exemple :

```python
from pyspark.sql import functions as F

df_customers = df_customers.withColumn(
    "company_name",
    F.trim(F.col("company_name"))
)
```

---

## 4. Enrichissement

Les données peuvent être enrichies avec des informations provenant de sources externes ou d'une API.

L'objectif est d'ajouter de nouvelles informations utiles aux analyses.

```text
Données internes
      +
Données externes
      ↓
Données enrichies
```

---

## 5. Transformation

Les données sont transformées avec **Apache Spark / PySpark** afin de produire des données prêtes à être exploitées.

Exemples :

* jointures entre tables ;
* calculs de montants ;
* agrégations ;
* création de nouvelles colonnes ;
* regroupement des données ;
* préparation des données pour l'analyse.

---

## 6. Orchestration

**Apache Airflow** permet d'automatiser l'exécution du pipeline.

Le workflow peut être représenté ainsi :

```text
Ingestion
    ↓
Validation
    ↓
Nettoyage
    ↓
Enrichissement
    ↓
Transformation
    ↓
Contrôle qualité
    ↓
Données finales
```

Airflow permet également de :

* planifier les traitements ;
* gérer les dépendances entre tâches ;
* détecter les erreurs ;
* relancer certaines tâches ;
* suivre l'état du pipeline.

---

# 🔐 Sécurité

La sécurité des données est prise en compte à plusieurs niveaux.

## Azure Key Vault

Les informations sensibles telles que les clés, mots de passe ou chaînes de connexion ne doivent pas être stockées directement dans le code.

**Azure Key Vault** permet de centraliser et protéger ces secrets.

```text
Application
     │
     ▼
Azure Key Vault
     │
     ▼
Secrets sécurisés
```

## Autres mesures

Le projet prend également en compte :

* la gestion des accès ;
* la protection des identifiants ;
* la séparation des données brutes et transformées ;
* la validation des données ;
* les logs ;
* la gestion des erreurs.

---

# 🐳 Docker

Les différents composants nécessaires au projet sont exécutés dans des conteneurs Docker.

L'utilisation de Docker permet :

* d'avoir un environnement reproductible ;
* de faciliter l'installation ;
* d'isoler les services ;
* de simplifier le déploiement ;
* de faciliter les tests.

Exemple de services utilisés :

```text
Docker Compose
│
├── Spark / Jupyter
├── PostgreSQL
├── pgAdmin
└── Airflow
```

---

# ⚡ Pourquoi Apache Spark ?

Le projet utilise **Apache Spark** pour traiter les données.

Une première analyse peut être réalisée avec Pandas pour de petits volumes.

Cependant, Spark est plus adapté lorsque le volume de données augmente grâce à son architecture distribuée.

### Comparaison simplifiée

| Pandas                               | Spark                                   |
| ------------------------------------ | --------------------------------------- |
| Traitement principalement en mémoire | Traitement distribué                    |
| Très pratique pour petits volumes    | Adapté aux volumes importants           |
| Simple à utiliser                    | Plus adapté aux traitements Big Data    |
| Exécution locale                     | Peut fonctionner sur plusieurs machines |

Dans le contexte de TradeCorp, Spark permet donc de préparer l'architecture à une augmentation importante du volume de données.

---

# 📊 Qualité des données

Des contrôles sont réalisés afin de vérifier la qualité des données.

Les principaux contrôles concernent :

* les valeurs nulles ;
* les doublons ;
* les types de données ;
* les dates ;
* les identifiants ;
* les valeurs aberrantes ;
* les espaces inutiles ;
* la cohérence entre les différentes tables.

L'objectif est de garantir que les données utilisées pour les analyses sont :

**fiables → cohérentes → propres → exploitables**

---

# 📈 Analyse exploratoire

L'analyse exploratoire est réalisée avec **Jupyter Notebook**, Pandas et/ou PySpark.

Elle permet notamment de :

* comprendre la structure des données ;
* connaître le nombre de lignes ;
* identifier les colonnes ;
* analyser les statistiques ;
* rechercher les valeurs manquantes ;
* détecter les anomalies ;
* comprendre les relations entre les données.

Exemples :

```python
df.printSchema()
```

```python
df.show()
```

```python
df.describe().show()
```

---

# 📁 Structure du projet

La structure du projet est organisée de manière à séparer les différentes responsabilités.

```text
projet-TradeCorp/
│
├── data/
│   ├── customers.csv
│   ├── orders.csv
│   ├── order_details.csv
│   ├── products.csv
│   ├── categories.csv
│   ├── suppliers.csv
│   ├── employees.csv
│   └── shippers.csv
│
├── notebooks/
│   └── analyses/
│
├── src/
│   ├── reader.py
│   ├── transformer.py
│   ├── enrichement.py
│   ├── writer.py
│   └── pipeline.py
│
├── dags/
│   └── tradecorp_pipeline.py
│
├── docker-compose.yml
├── requirements.txt
├── .env
├── .gitignore
└── README.md
```

> La structure peut évoluer selon l'organisation finale du projet.

---

# 🚀 Installation

## Prérequis

Les éléments suivants sont nécessaires :

* Docker Desktop ;
* Git ;
* un compte Azure ;
* Python ;
* VS Code recommandé.

---

## 1. Cloner le projet

```bash
git clone <URL_DU_REPOSITORY>
```

Puis :

```bash
cd projet-TradeCorp
```

---

## 2. Configurer les variables d'environnement

Créer un fichier `.env`.

Exemple :

```env
AZURE_STORAGE_ACCOUNT_URL=...
AZURE_STORAGE_ACCOUNT_NAME=...
AZURE_STORAGE_CONTAINER_NAME=...
AZURE_CLIENT_ID=...
AZURE_TENANT_ID=...
AZURE_CLIENT_SECRET=...
```

⚠️ **Ne jamais publier les secrets dans GitHub.**

Le fichier `.env` doit être ajouté au `.gitignore`.

---

## 3. Démarrer les conteneurs

```bash
docker compose up -d
```

Vérifier les conteneurs :

```bash
docker compose ps
```

---

## 4. Vérifier Spark

```bash
docker exec -it tradecorp_training_spark python -c "import pyspark; print(pyspark.__version__)"
```

---

## 5. Vérifier Java

Spark nécessite Java.

```bash
docker exec -it tradecorp_training_spark readlink -f /usr/bin/java
```

Exemple de résultat :

```text
/usr/lib/jvm/java-17-openjdk-amd64/bin/java
```

---

# ▶️ Exécution du pipeline

Le pipeline peut être exécuté depuis le conteneur ou via Airflow selon la configuration finale du projet.

Exemple :

```bash
docker exec -it tradecorp_training_spark python /home/jovyan/work/src/pipeline.py
```

Pour Airflow, les DAGs sont placés dans le répertoire :

```text
dags/
```

Puis Airflow permet de lancer et suivre automatiquement le pipeline.

---

# 🧪 Tests et contrôles

Avant de considérer le pipeline comme fonctionnel, plusieurs contrôles sont réalisés :

* vérification du chargement des fichiers ;
* vérification du schéma ;
* contrôle du nombre de lignes ;
* contrôle des valeurs nulles ;
* contrôle des doublons ;
* contrôle des transformations ;
* contrôle des erreurs ;
* vérification du résultat final.

---

# 🌱 Green IT

Le projet prend en compte les principes du numérique responsable.

Les principales actions sont :

* éviter les traitements inutiles ;
* limiter les ressources utilisées ;
* optimiser les traitements Spark ;
* arrêter les conteneurs lorsqu'ils ne sont pas utilisés ;
* éviter la duplication inutile des données ;
* contrôler le stockage ;
* prévoir l'archivage et la suppression des données devenues inutiles.

L'objectif est de trouver un équilibre entre **performance, coût et consommation de ressources**.

---

# 💰 Gestion des coûts

L'utilisation du cloud entraîne des coûts liés notamment :

* au stockage ;
* au calcul ;
* au réseau ;
* aux services Azure utilisés.

Les coûts doivent être estimés avant le déploiement et suivis pendant l'exploitation.

Une estimation peut être réalisée avec **Azure Pricing Calculator**.

---

# 📊 Indicateurs de suivi

Quelques KPI peuvent être utilisés pour mesurer la qualité du projet :

| Indicateur                 | Objectif                       |
| -------------------------- | ------------------------------ |
| Temps d'exécution          | Réduire le temps de traitement |
| Nombre de fichiers traités | Vérifier l'ingestion           |
| Nombre d'erreurs           | Garantir la fiabilité          |
| Nombre de lignes traitées  | Suivre les volumes             |
| Données invalides          | Mesurer la qualité             |
| Coût Azure                 | Contrôler les dépenses         |
| Utilisation des ressources | Optimiser l'infrastructure     |

---

# 🔮 Évolutions possibles

Plusieurs améliorations peuvent être envisagées :

* mise en place d'un véritable environnement de production ;
* ajout d'un système de monitoring ;
* ajout d'alertes en cas d'échec du pipeline ;
* utilisation d'un format de données optimisé comme Parquet ;
* mise en place d'une architecture Data Lakehouse ;
* ajout d'un Data Warehouse ;
* création de dashboards Power BI ;
* amélioration de la gouvernance des données ;
* ajout de tests automatisés ;
* mise en place d'une CI/CD ;
* amélioration de la scalabilité du traitement Spark.

---

# 📚 Documentation

La documentation du projet comprend notamment :

* documentation d'architecture ;
* documentation technique ;
* documentation du pipeline ;
* documentation des traitements PySpark ;
* guide d'installation ;
* guide d'utilisation ;
* documentation de la sécurité ;
* documentation des choix technologiques.

---

# 👨‍💻 Auteur

**Projet : TradeCorp Data Platform**

Projet réalisé dans le cadre de la formation **Data Engineer**.

Technologies principales :

**Python · PySpark · Apache Spark · Apache Airflow · Azure · ADLS Gen2 · Docker · PostgreSQL · Git**

---

# 📌 Résumé

TradeCorp Data Platform permet de passer d'un traitement manuel des données commerciales à une architecture **automatisée, sécurisée et évolutive**.

```text
              TRADECORP DATA PLATFORM

CSV
 │
 ▼
Azure ADLS Gen2
 │
 ▼
Airflow
 │
 ▼
PySpark / Spark
 │
 ├── Nettoyage
 ├── Enrichissement
 └── Transformation
 │
 ▼
Données fiables
 │
 ▼
Analyse / BI
```

L'objectif final est de fournir à TradeCorp une plateforme permettant de **traiter les données plus rapidement, plus fiablement et avec une architecture capable d'évoluer avec les besoins de l'entreprise**.
