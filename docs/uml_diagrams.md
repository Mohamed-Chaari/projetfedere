# Diagrammes UML - Météo Tunisie

Ce document présente l'architecture et les flux principaux de la plateforme **Météo Tunisie** sous forme de diagrammes UML (Mermaid.js).

---

## 1. Diagramme des Cas d'Utilisation (Use Case)

Ce diagramme illustre les interactions possibles des utilisateurs (Administrateur et Utilisateur Public) avec le système.

```mermaid
flowchart LR
    User((Utilisateur Public))
    Admin((Administrateur))
    API[[Système Externe<br/>Open-Meteo API]]

    subgraph Météo Tunisie
        UC1([Consulter le tableau de bord])
        UC2([Voir les conditions actuelles])
        UC3([Consulter les prévisions à 7 jours])
        UC4([Voir les alertes météo])
        UC5([Analyser les tendances])

        UC6([Ingérer les données])
        UC7([Superviser les jobs Airflow])
        UC8([Gérer le pipeline de données])
    end

    User --> UC1
    User --> UC2
    User --> UC3
    User --> UC4
    User --> UC5

    Admin --> UC7
    Admin --> UC8
    Admin --> UC1

    API --> UC6
    UC8 -. "« include »" .-> UC6
```

---

## 2. Diagramme de Classes (Class Diagram)

Ce diagramme offre une vue de haut niveau sur l'architecture backend, le schéma de base de données (TimescaleDB) et les composants de traitement (FastAPI et Kafka).

```mermaid
classDiagram
    %% Base de données (Schéma PostgreSQL / TimescaleDB)
    class WeatherHistorical {
        +BigInt id
        +Date date
        +String city
        +String governorate
        +String region
        +Numeric temperature
        +Numeric precipitation
        +SmallInt weather_code
        +TimestampTz ingested_at
    }

    class WeatherCurrent {
        +BigInt id
        +String city
        +Numeric temperature
        +Numeric humidity
        +Numeric wind_speed
        +TimestampTz observed_at
    }

    class WeatherForecast {
        +BigInt id
        +String city
        +Date forecast_for
        +SmallInt forecast_day
        +Numeric temp_max
        +Numeric temp_min
    }

    class WeatherAlerts {
        +BigInt id
        +String alert_type
        +String severity
        +String city
        +Numeric value
        +Numeric threshold
        +TimestampTz triggered_at
    }

    class MonthlyAverages {
        +SmallInt year
        +SmallInt month
        +String city
        +Numeric avg_temp
    }

    %% Composants du Pipeline de Données
    class KafkaProducer {
        +fetch_historical()
        +fetch_current()
        +fetch_forecast()
        +check_and_publish_alerts()
        +publish()
    }

    class KafkaConsumer {
        +consume_topic()
        +run_batch()
        +run_continuous()
        -_flush_batch()
    }

    %% Composants API (FastAPI)
    class FastAPIRouter {
        +get_dashboard_summary()
        +get_forecast(city)
        +get_alerts()
        +get_analysis()
    }

    class DatabaseService {
        +get_db()
        +execute_query(query, params)
        +health_check()
    }

    %% Relations
    KafkaProducer --> KafkaConsumer : Envoie messages (Topics)
    KafkaConsumer --> WeatherHistorical : Upsert / Insert
    KafkaConsumer --> WeatherCurrent : Upsert / Insert
    KafkaConsumer --> WeatherForecast : Upsert / Insert
    KafkaConsumer --> WeatherAlerts : Insert

    DatabaseService --> WeatherHistorical : Requête
    DatabaseService --> WeatherCurrent : Requête
    DatabaseService --> WeatherForecast : Requête
    DatabaseService --> MonthlyAverages : Requête
    DatabaseService --> WeatherAlerts : Requête

    FastAPIRouter --> DatabaseService : Utilise execute_query()
```

---

## 3. Diagrammes de Séquence (Sequence Diagrams)

### 3.1. Pipeline d'ingestion de données

Ce diagramme montre le cheminement des données depuis l'API source (Open-Meteo) jusqu'au stockage PostgreSQL (TimescaleDB).

```mermaid
sequenceDiagram
    autonumber
    participant APISrc as Open-Meteo API
    participant Prod as Kafka Producer
    participant Kafka as Kafka Broker
    participant Cons as Kafka Consumer
    participant DB as PostgreSQL/TimescaleDB

    Prod->>APISrc: Requête API (Météo Actuelle / Prévisions / Historique)
    APISrc-->>Prod: Réponse JSON
    Prod->>Prod: Validation et Formatage (build_message)
    Prod->>Kafka: Publie message (Topic: weather-current, etc.)
    Kafka-->>Prod: Acknowledgment (Ack)

    Cons->>Kafka: Poll (Récupère les messages par lot)
    Kafka-->>Cons: Liste de messages
    Cons->>Cons: Normalisation des timestamps
    Cons->>DB: _flush_batch() (Exécute UPSERT / INSERT)
    DB-->>Cons: Confirmation d'écriture
    Cons->>Kafka: Commit de l'offset
```

### 3.2. Interaction du tableau de bord utilisateur

Ce diagramme détaille le flux lorsqu'un utilisateur interagit avec l'interface frontend pour récupérer les données via l'API FastAPI.

```mermaid
sequenceDiagram
    autonumber
    actor User as Utilisateur
    participant JS as Frontend (Vanilla JS)
    participant API as FastAPI Backend
    participant DB as PostgreSQL/TimescaleDB

    User->>JS: Charge la page (ex: realtime.html)
    JS->>API: GET /api/dashboard/summary (Fetch API)
    API->>DB: execute_query("SELECT * FROM v_national_summary...")
    DB-->>API: Retourne les enregistrements
    API-->>JS: Réponse JSON (200 OK)
    JS->>JS: Met à jour le DOM (Leaflet, Plotly, KPIs)
    JS-->>User: Affiche les données à l'écran
```

---

## 4. Diagrammes d'Activité (Activity Diagrams)

### 4.1. Processus d'Ingestion des Données (Producteur)

```mermaid
stateDiagram-v2
    [*] --> Démarrage
    Démarrage --> FetchData: Récupérer les données depuis l'API Open-Meteo

    state FetchData {
        [*] --> Requête_HTTP
        Requête_HTTP --> Succès: Code 200
        Requête_HTTP --> Échec: Erreur Réseau/Timeout
        Échec --> Requête_HTTP: Retry (si max_retries non atteint)
    }

    FetchData --> Validation: Formatage JSON et validation
    Validation --> Topic_Kafka: Publication sur le Topic approprié
    Topic_Kafka --> CheckAlerts: Données "Current" uniquement

    state CheckAlerts {
        [*] --> Évaluer_Seuils
        Évaluer_Seuils --> Alerte_Détectée: Température/Vent hors norme
        Évaluer_Seuils --> Fin_Vérif: Pas d'anomalie
        Alerte_Détectée --> Kafka_Alert: Publication sur weather-alerts
        Kafka_Alert --> Fin_Vérif
    }

    CheckAlerts --> Attente: Pause jusqu'au prochain cycle
    Attente --> Démarrage
```

### 4.2. Processus d'Interaction avec le Dashboard (Frontend)

```mermaid
stateDiagram-v2
    [*] --> Chargement_Page
    Chargement_Page --> Appel_API_Init: Requête Fetch vers /api/dashboard/summary

    Appel_API_Init --> Succès_API: Statut 200
    Appel_API_Init --> Erreur_API: Statut 5xx / 4xx ou Timeout

    Succès_API --> Rendu_KPIs: Mise à jour des cartes métriques
    Succès_API --> Rendu_Carte: Mise à jour Leaflet.js
    Succès_API --> Rendu_Graphes: Mise à jour Plotly.js

    Rendu_KPIs --> Fin_Affichage
    Rendu_Carte --> Fin_Affichage
    Rendu_Graphes --> Fin_Affichage

    Erreur_API --> Affichage_Erreur: Notification utilisateur (Erreur de chargement)
    Affichage_Erreur --> Fin_Affichage

    Fin_Affichage --> [*]
```

---

## 5. Diagramme de Déploiement (Deployment Diagram)

Ce diagramme illustre la façon dont l'application est déployée sur différents conteneurs (via Docker Compose).

```mermaid
flowchart TD
    subgraph Client [Navigateur Web]
        UI[Interface Vanilla JS / HTML]
    end

    subgraph DockerHost [Hôte Docker (Serveur / Local)]

        subgraph ServeurWeb [Conteneur Nginx]
            Nginx[Nginx : Sert les fichiers statiques]
        end

        subgraph Backend [Conteneur FastAPI]
            API[FastAPI Uvicorn]
        end

        subgraph Pipeline [Conteneurs Python]
            Prod[Kafka Producer]
            Cons[Kafka Consumer]
        end

        subgraph MessageBroker [Conteneur Kafka]
            Zook[Zookeeper] --- KBroker[Kafka Broker]
        end

        subgraph BaseDeDonnees [Conteneur PostgreSQL]
            DB[(PostgreSQL 15 + TimescaleDB)]
        end

        subgraph Orchestration [Conteneurs Airflow]
            AirflowUI[Airflow Webserver]
            AirflowSched[Airflow Scheduler]
        end
    end

    Client -->|HTTP/REST| ServeurWeb
    ServeurWeb -->|Proxy Inversé| API

    API -->|TCP / psycopg2| DB
    Prod -->|TCP| KBroker
    KBroker -->|TCP| Cons
    Cons -->|TCP / psycopg2| DB
    AirflowSched -->|Exécute DAGs / db.execute_query| DB
```
