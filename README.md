<img width="1536" height="1024" alt="miniature_youtube" src="https://github.com/user-attachments/assets/bd32b5b7-fec3-4ebb-bb93-45f6458527ab" />

# NYC Yellow Taxi Analytics with Databricks

[Regarder la vidéo du Projet](https://youtu.be/eNSqtSDjkl0)

Bienvenue dans ce projet de **Data Engineering & Analytics** construit sur **Databricks**.
L’objectif est de montrer comment mettre en place un pipeline **end-to-end** (Bronze → Silver → Gold) pour transformer, nettoyer et analyser les données des taxis jaunes de New York, puis les exposer dans un **dashboard interactif**.

Ce projet illustre :

* Maîtrise des **bonnes pratiques en architecture Data Lakehouse** (Bronze/Silver/Gold).
* Automatisation via **Databricks Workflows**.
* Création d’un **dashboard analytique** sur des millions de courses de taxi.
* Capacité à **expliquer clairement un projet technique** de bout en bout.

---

## Objectifs pédagogiques

1. **Ingestion (Bronze)** : stocker les données brutes sans altération.
2. **Nettoyage & Transformation (Silver)** : appliquer des règles métier (formats de dates, cohérence des colonnes, gestion des valeurs aberrantes).
3. **Modèle analytique (Gold)** : agréger et enrichir les données pour les rendre exploitables par les analystes et décideurs.
4. **Visualisation** : fournir un **dashboard clair et interactif** permettant de répondre aux questions business (tendances, distances moyennes, revenus par zone…).
5. **Orchestration & Scheduling** : automatiser le pipeline complet avec un workflow Databricks.

---

## Architecture & Workflow

Le pipeline est organisé en **3 couches** (Bronze → Silver → Gold) et orchestré via un **Databricks Workflow**.

<img width="992" height="145" alt="workflows" src="https://github.com/user-attachments/assets/2656ef32-5ba0-447e-bde8-0902c960e7ee" />

* **Bronze Layer** : ingestion brute des fichiers Yellow Taxi (CSV/Parquet).
* **Silver Layer** : nettoyage, normalisation et enrichissement (ajout de colonnes calculées comme `weekday_name`, `route`).
* **Gold Layer** : tables analytiques prêtes à l’emploi pour le dashboard.

---

## Dashboard

Le résultat final est un **dashboard interactif** qui permet d’analyser les courses de taxi sous différents angles :

* **Nombre de trajets** par jour, par heure.
* **Distance moyenne** par route.
* **Revenus totaux** par combinaison pickup/dropoff.
* Analyse des pics de demande selon les horaires et jours de semaine.

<img width="1594" height="848" alt="dashboard_databricks" src="https://github.com/user-attachments/assets/a09401e8-efda-4930-9138-76ce66f9f646" />

---

## Points clés mis en avant

- Mise en place d’un **pipeline moderne Lakehouse**.
- Respect des **bonnes pratiques de Data Engineering** (Bronze/Silver/Gold).
- **Automatisation complète** avec Workflows.
- Capacité à transformer un jeu de données brut en **insights business concrets**.
- Documentation et pédagogie adaptées à un **contexte professionnel**.

---

## Stack technique

* **Databricks** (Delta Lake, Workflows, Notebooks)
* **PySpark** pour la transformation de données
* **SQL** pour les agrégations analytiques
* **Dashboarding natif Databricks**

---

## À propos de moi :

![20211207_122627](https://github.com/user-attachments/assets/3ebfddaf-7234-428c-bda6-c0d43f203fb2)


Je suis **Consultant Data Engineer / Data Scientist** et je conçois des pipelines et solutions analytiques **de bout en bout**.
Ce projet est un exemple de ma capacité à :

* Construire des pipelines robustes,
* Automatiser les traitements de données,
* Valoriser les données en analyses business utiles.

---

👉 Si tu es recruteur ou data enthusiast, n’hésite pas à me contacter pour échanger autour de ce projet ou de futurs challenges.
