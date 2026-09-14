# 🚀 TradeCorp Data Platform

## 📋 Présentation

**TradeCorp Data Platform** est une plateforme de Data Engineering moderne conçue pour automatiser, nettoyer et valoriser l'ensemble des flux de données commerciales et financières de l'entreprise.

Actuellement, le traitement partiel et manuel des données entraîne :
* un temps de traitement important et un risque d'erreurs humaines ;
* une incapacité à passer à l'échelle sur de gros volumes ;
* des difficultés de traçabilité, de suivi et de maintenance.

Ce projet résout ces contraintes grâce à un **pipeline ETL conteneurisé, robuste, automatisé et scalable**, basé sur une architecture **Lakehouse Medallion (Bronze / Silver)**.

---

# 🎯 Objectifs du projet

* **Centralisation & Ingestion Multi-Sources :** Automatiser la collecte de données décisionnelles (fichiers CSV) et de flux financiers externes (taux de change JSON/API).
* **Stockage Cloud Sécurisé :** Structurer les données sur Azure ADLS Gen2 selon le modèle Medallion (`raw` / `clean`).
* **Transformation à Haute Performance :** Nettoyer, dédupliquer, joindre et enrichir les flux métier en utilisant **PySpark** sur une modélisation en étoile/flocon (*Snowflake Schema*).
* **Orchestration Conteneurisée :** Automatiser les dépendances des workflows via **Apache Airflow** et des conteneurs isolés via `DockerOperator`.
* **Haute Disponibilité & Fallback :** Garantir la continuité de service (ex: bascule automatique de l'ingestion vers un stockage local si l'accès cloud échoue).
* **Qualité & Green IT :** Valider les traitements par des tests unitaires PyTest et optimiser l'empreinte temporaire E/S (`shutil.rmtree()`).

---

# 🏗️ Architecture du projet (Lakehouse Medallion)

```text
 ┌───────────────────────────┐      ┌───────────────────────────┐
 │   Fichiers CSV (Métier)   │      │ Flux JSON (API Change)    │
 └─────────────┬─────────────┘      └─────────────┬─────────────┘
               │                                  │
               └────────────────┬─────────────────┘
                                ▼
         ┌──────────────────────────────────────────────┐
         │     Azure ADLS Gen2 - Zone BRONZE (raw)      │
         │         (Bascule de fallback local)          │
         └──────────────────────┬───────────────────────┘
                                │
                                ▼
         ┌──────────────────────────────────────────────┐
         │          Orchestration Apache Airflow        │
         │           (DAGs via DockerOperator)          │
         └──────────────────────┬───────────────────────┘
                                │
                                ▼
         ┌──────────────────────────────────────────────┐
         │             Moteur PySpark (Spark 3.x)       │
         │  - Nettoyage (initcap, upper, to_date)       │
         │  - Jointures Snowflake (build_enriched)      │
         │  - Enrichissement Multi-Devises (hasattr)   │
         └──────────────────────┬───────────────────────┘
                                │
                                ▼
         ┌──────────────────────────────────────────────┐
         │     Azure ADLS Gen2 - Zone SILVER (clean)    │
         │      Format Parquet Compressé (Snappy)       │
         └──────────────────────┬───────────────────────┘
                                │
                                ▼
         ┌──────────────────────────────────────────────┐
         │      Exploitation Analytics / BI / Databricks│
         └──────────────────────────────────────────────┘

🛠️ Technologies utiliséesTechnologieRole & UtilisationPython 3.xLangage principal des scripts d'ETL et de testPySpark / Apache Spark 3.xMoteur de calcul distribué (Transformations & Jointures)Apache AirflowOrchestration et planification quotidienne des workflows (DockerOperator)Azure ADLS Gen2Storage Lakehouse Cloud (Containers raw et clean)Azure Key Vault / .envGestion et sécurisation des secrets et identifiantsDocker / Docker ComposeConteneurisation isolée et reproductibilité de l'environnementPyTestSuite de tests unitaires et validation in-memory du code SparkParquet (Snappy)Format de stockage orienté colonne optimisé pour la Zone SilverGit / GitHubControl de version et suivi de dev📂 Sources de donnéesPlaintextdata/
├── categories.csv         # Catégories de produits
├── customers.csv          # Référentiel clients
├── employees.csv          # Employés et vendeurs
├── exchange_rates.json    # Flux de taux de change (JSON structuré)
├── order_details.csv      # Lignes de commandes & remises
├── orders.csv             # Entêtes de commandes
├── products.csv           # Catalogue produits
├── shippers.csv           # Transporteurs
└── country_currency.csv   # Mapping Pays / Devise

🔄 Pipeline de données & Règles Métier1. Ingestion & Resilience (Fallback)Les données sont ingérées vers la zone Bronze (raw). Pour l'ingestion des devises (exchange_rates.json), si la connexion Azure est interrompue, le pipeline bascule automatiquement sur un fichier de secours local (/home/jovyan/data/exchange_rates.json).2. Nettoyage & StandardisationClients : Déduplication sur customer_id, mise en casse propre (initcap) des noms et passage en majuscules (upper) des pays.Commandes : Conversion des types texte vers DateType (F.to_date()) et calcul de l'indicateur booléen is_shipped.Détails Ventes : Calcul du sous-total net avec remise appliquée :$$\text{sous\_total} = \text{round}(\text{prix\_unitaire} \times \text{quantite} \times (1 - \text{discount}), 2)$$3. Grande Jointure Snowflake (build_enriched)La table de faits (order_details + orders) est consolidée avec les tables de dimensions (customers, products, categories, employees, shippers) via des jointures à gauche (LEFT JOIN) intégrant un renommage préventif des colonnes ambiguës (ex: company_name $\rightarrow$ customer_name).4. Enrichissement Multi-Devises (enrichment.py)Gestion des schémas JSON complexes via analyse dynamique PySpark (hasattr(schema, "fields")) supportant les formats STRUCT, MAP et plat.Conversion monétaire dynamique et fallback automatique sur un taux égal à 1.0 ou la monnaie par défaut USD via F.coalesce().5. Stockage Zone Silver (Parquet)Les jeux de données enrichis sont sauvegardés au format Parquet compressé (Snappy), filtrant automatiquement les fichiers résiduels Spark (_SUCCESS, .crc).

🐳 Conteneurisation & Isolation DockerToutes les briques du projet tournent sous Docker pour garantir le zéro conflit d'environnement.PlaintextDocker Stack
│
├── Airflow Webserver & Scheduler
├── Spark / Jupyter (projet-tradecorp-spark)
└── PostgreSQL (Metastore Airflow)

Chaque tâche exécutée par Airflow instancie un conteneur éphémère grâce au DockerOperator (auto_remove=True), garantissant une isolation stricte des dépendances et un nettoyage complet des ressources après exécution.🧪 Qualité du Code & Tests UnitairesLa suite de tests unitaires est gérée avec PyTest dans le dossier tests/.Fixture Spark Session : Utilisation d'une session Spark unique à portée de session (scope="session") pour optimiser la vitesse d'exécution.Isolation In-Memory : Les tests de transformation (test_transformers.py) s'exécutent en mémoire sans dépendance réseau ou cloud.Pour lancer les tests :Bashdocker exec -it tradecorp_training_spark pytest tests/

🌱 Numérique Responsable (Green IT)Nettoyage I/O Systématique : Utilisation de shutil.rmtree() dans les dossiers temporaires du conteneur (/tmp/data/) avant écriture pour éviter la saturation du disque.Conteneurs Éphémères : Destruction automatique des conteneurs de traitement Airflow après exécution.Format Parquet : Réduction significative de l'empreinte de stockage et de la consommation d'E/S réseau par rapport au stockage CSV brut.

📁 Structure du projetPlaintextprojet-TradeCorp/
│
├── data/                  # Fichiers bruts CSV et JSON de secours
├── dags/                  # DAGs Apache Airflow (tradecorp_pipeline.py)
├── notebooks/             # Exploration préliminaire Jupyter
├── src/                   # Core Code PySpark
│   ├── reader.py          # Module d'ingestion & Fallbacks
│   ├── transformer.py     # Métier & Grande Jointure Snowflake
│   ├── enrichment.py      # Dépilage JSON/STRUCT & Taux de change
│   └── writer.py          # Export Parquet & Nettoyage I/O
├── tests/                 # Tests unitaires PyTest
│   └── test_transformers.py
├── docker-compose.yml     # Configuration multi-services
├── Dockerfile             # Image custom PySpark / Spark 3.x
├── requirements.txt       # Dépendances Python
├── .env.example           # Modèle de variables d'environnement
├── .gitignore
└── README.md

🚀 Déploiement & Démarrage1. Cloner le projetBashgit clone <URL_DU_REPOSITORY>

cd projet-TradeCorp

2. Configurer les variables d'environnementCréer un fichier .env basé sur .env.example :Extrait de codeAZURE_STORAGE_ACCOUNT_NAME=your_account_name

AZURE_STORAGE_CONTAINER_NAME=your_container
AZURE_CLIENT_ID=your_client_id
AZURE_TENANT_ID=your_tenant_id
AZURE_CLIENT_SECRET=your_client_secret

3. Lancer l'environnement DockerBashdocker compose up -d

4. Lancer le pipeline manuellement (Optionnel)Bashdocker exec -it tradecorp_training_spark python /home/jovyan/work/src/pipeline.py
👨‍💻 AuteurProjet TradeCorp Data PlatformRéalisé dans le cadre de la formation Data Engineer.Stack : PySpark • Apache Airflow • Azure ADLS Gen2 • Docker • PyTest • Parquet