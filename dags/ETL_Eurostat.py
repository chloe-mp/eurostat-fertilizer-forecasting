import logging
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

default_args = {
    "owner": "chloe",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="engrais_etl_eurostat_complet",
    default_args=default_args,
    description="ETL Eurostat : engrais minéraux + bilan nutritif (incluant organiques)",
    schedule_interval="@weekly",
    start_date=datetime(2026, 1, 13),
    catchup=False,
    tags=["eurostat", "engrais", "agriculture"],
)

TMP_MINERAL_PATH = Path("/opt/airflow/data/eurostat_mineral_latest.csv")
TMP_GNB_PATH = Path("/opt/airflow/data/eurostat_gnb_latest.csv")


def fetch_eurostat_dataset(dataset_code, tmp_path):
    """
    Fetch données Eurostat via l'API JSON (plus stable que le TSV bulk).
    """
    # URL de l'API JSON 1.0
    url = f"https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/{dataset_code}"

    # Paramètres pour éviter les erreurs 406/400
    params = {"format": "JSON", "lang": "en"}

    try:
        logging.info(f"Téléchargement Eurostat {dataset_code} via JSON API...")
        response = requests.get(url, params=params, timeout=120)
        response.raise_for_status()
        json_data = response.json()

        # --- PARSING DU JSON EUROSTAT ---
        # Eurostat sépare les dimensions (pays, temps, etc.) des valeurs
        values = json_data["value"]  # Dictionnaire { "0": 12.5, "1": 14.2 ... }
        dimensions = json_data["dimension"]

        # Récupération des listes d'IDs pour chaque dimension
        # Attention : l'ordre des dimensions varie selon le dataset
        id_list = json_data["id"]  # ex: ['freq', 'nutrient', 'unit', 'geo', 'time']

        # Préparation des labels pour chaque dimension
        dim_map = {}
        for d in id_list:
            dim_map[d] = list(dimensions[d]["category"]["index"].keys())

        # Reconstruction des lignes
        rows = []
        for idx_str, val in values.items():
            idx = int(idx_str)

            # Calcul des index pour retrouver les dimensions
            current_idx = idx
            row = {"valeur": val}

            # Cette boucle permet de retrouver quel pays/année correspond à la valeur
            for d in reversed(id_list):
                size = len(dim_map[d])
                row[d] = dim_map[d][current_idx % size]
                current_idx //= size
            rows.append(row)

        # Création du DataFrame Polars
        df_long = pl.DataFrame(rows)

        # --- ADAPTATION AU FORMAT DE LA BASE DE DONNÉES ---
        # On renomme pour coller aux tables SQL existantes
        if dataset_code == "aei_fm_usefert":
            df_long = df_long.select(
                [
                    pl.col("time").alias("annee").cast(pl.Int32),
                    pl.col("geo").alias("pays"),
                    pl.col("nutrient").alias("nutriment"),
                    pl.col("valeur").cast(pl.Float64),
                    pl.lit("KG_HA").alias("unite"),  # On force l'unité ici
                ]
            )

        elif dataset_code == "aei_pr_gnb":
            df_long = df_long.select(
                [
                    pl.col("time").alias("annee").cast(pl.Int32),
                    pl.col("geo").alias("pays"),
                    pl.col("nutrient").alias("nutriment"),
                    pl.col("indic_ag").alias("indicateur"),
                    pl.col("valeur").cast(pl.Float64),
                    pl.lit("KG_HA").alias("unite"),  # On force l'unité ici
                ]
            )

        # Sauvegarde en CSV pour le load_mineral / load_gnb (COPY command)
        df_long.write_csv(tmp_path)
        logging.info(f"{dataset_code}: {df_long.shape[0]} lignes sauvegardées.")

    except Exception as e:
        logging.error(f"Erreur fetch {dataset_code}: {str(e)}")
        raise


extract_mineral = PythonOperator(
    task_id="extract_mineral",
    python_callable=fetch_eurostat_dataset,
    op_kwargs={"dataset_code": "aei_fm_usefert", "tmp_path": TMP_MINERAL_PATH},
    dag=dag,
)

extract_gnb = PythonOperator(
    task_id="extract_gnb_balance",
    python_callable=fetch_eurostat_dataset,
    op_kwargs={"dataset_code": "aei_pr_gnb", "tmp_path": TMP_GNB_PATH},
    dag=dag,
)

create_tables = SQLExecuteQueryOperator(
    task_id="create_tables",
    conn_id="postgres_default",
    sql="""
    CREATE TABLE IF NOT EXISTS eurostat_mineral (
        annee INTEGER,
        pays TEXT,
        nutriment TEXT,
        valeur NUMERIC,
        unite TEXT,
        inserted_at TIMESTAMP DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS eurostat_gnb (
        annee INTEGER,
        pays TEXT,
        nutriment TEXT,
        indicateur TEXT,
        valeur NUMERIC,
        unite TEXT,
        inserted_at TIMESTAMP DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS eurostat_engrais_final (
        annee INTEGER,
        pays TEXT,
        nutriment TEXT,
        valeur_mineral NUMERIC,
        valeur_bilan_inputs NUMERIC,
        valeur_bilan_outputs NUMERIC,
        valeur_bilan_net NUMERIC,
        unite TEXT,
        last_updated TIMESTAMP DEFAULT NOW(),
        PRIMARY KEY (annee, pays, nutriment)
    );
    """,
    autocommit=True,
)

load_mineral = SQLExecuteQueryOperator(
    task_id="load_mineral",
    conn_id="postgres_default",
    sql=f"""
    TRUNCATE eurostat_mineral;
    COPY eurostat_mineral (annee, pays, nutriment, valeur, unite)
    FROM '{TMP_MINERAL_PATH}'
    DELIMITER ',' CSV HEADER;
    """,
    autocommit=True,
)

load_gnb = SQLExecuteQueryOperator(
    task_id="load_gnb",
    conn_id="postgres_default",
    sql=f"""
    TRUNCATE eurostat_gnb;
    COPY eurostat_gnb (annee, pays, nutriment, indicateur, valeur, unite)
    FROM '{TMP_GNB_PATH}'
    DELIMITER ',' CSV HEADER;
    """,
    autocommit=True,
)

upsert_final = SQLExecuteQueryOperator(
    task_id="upsert_final",
    conn_id="postgres_default",
    sql="""
    WITH mineral_agg AS (
        SELECT annee, pays, nutriment, AVG(valeur) as valeur_mineral, MAX(unite) as unite
        FROM eurostat_mineral
        GROUP BY annee, pays, nutriment
    ),
    gnb_inputs AS (
        SELECT annee, pays, nutriment, AVG(valeur) as valeur_inputs, MAX(unite) as unite
        FROM eurostat_gnb
        WHERE indicateur LIKE '%INP%'
        GROUP BY annee, pays, nutriment
    ),
    gnb_outputs AS (
        SELECT annee, pays, nutriment, AVG(valeur) as valeur_outputs, MAX(unite) as unite
        FROM eurostat_gnb
        WHERE indicateur LIKE '%OUT%'
        GROUP BY annee, pays, nutriment
    ),
    gnb_balance AS (
        SELECT annee, pays, nutriment, AVG(valeur) as valeur_balance, MAX(unite) as unite
        FROM eurostat_gnb
        WHERE indicateur LIKE '%BAL%'
        GROUP BY annee, pays, nutriment
    )
    INSERT INTO eurostat_engrais_final (annee, pays, nutriment, valeur_mineral, valeur_bilan_inputs, valeur_bilan_outputs, valeur_bilan_net, unite)
    SELECT
        COALESCE(m.annee, i.annee, o.annee, b.annee) as annee,
        COALESCE(m.pays, i.pays, o.pays, b.pays) as pays,
        COALESCE(m.nutriment, i.nutriment, o.nutriment, b.nutriment) as nutriment,
        m.valeur_mineral,
        i.valeur_inputs,
        o.valeur_outputs,
        b.valeur_balance,
        COALESCE(m.unite, i.unite, o.unite, b.unite) as unite
    FROM mineral_agg m
    FULL OUTER JOIN gnb_inputs i
        ON m.annee = i.annee AND m.pays = i.pays AND m.nutriment = i.nutriment
    FULL OUTER JOIN gnb_outputs o
        ON COALESCE(m.annee, i.annee) = o.annee
        AND COALESCE(m.pays, i.pays) = o.pays
        AND COALESCE(m.nutriment, i.nutriment) = o.nutriment
    FULL OUTER JOIN gnb_balance b
        ON COALESCE(m.annee, i.annee, o.annee) = b.annee
        AND COALESCE(m.pays, i.pays, o.pays) = b.pays
        AND COALESCE(m.nutriment, i.nutriment, o.nutriment) = b.nutriment
    ON CONFLICT (annee, pays, nutriment)
    DO UPDATE SET
        valeur_mineral = EXCLUDED.valeur_mineral,
        valeur_bilan_inputs = EXCLUDED.valeur_bilan_inputs,
        valeur_bilan_outputs = EXCLUDED.valeur_bilan_outputs,
        valeur_bilan_net = EXCLUDED.valeur_bilan_net,
        unite = EXCLUDED.unite,
        last_updated = NOW();
    """,
    autocommit=True,
)

trigger_ml = TriggerDagRunOperator(
    task_id="trigger_ml_dag",
    trigger_dag_id="engrais_ml_eurostat",
    wait_for_completion=False,
    dag=dag,
)

(
    [extract_mineral, extract_gnb]
    >> create_tables
    >> [load_mineral, load_gnb]
    >> upsert_final
    >> trigger_ml
)
