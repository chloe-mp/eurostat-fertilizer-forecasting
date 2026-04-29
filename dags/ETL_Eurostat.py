"""ETL DAG for Eurostat fertilizer datasets.

Fetches mineral fertilizer use (aei_fm_usefert) and Gross Nutrient Balance
(aei_pr_gnb), parses Eurostat's JSON-stat 1.0 format, loads into PostgreSQL
via COPY, then merges into a unified per-country/nutrient/year table.
"""

import logging
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from lib.eurostat_parser import parse_eurostat_json

default_args = {
    "owner": "chloe",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="engrais_etl_eurostat_complet",
    default_args=default_args,
    description="ETL Eurostat: mineral fertilizers + nutrient balance (incl. organic).",
    schedule_interval="@weekly",
    start_date=datetime(2026, 1, 13),
    catchup=False,
    tags=["eurostat", "engrais", "agriculture"],
)

TMP_MINERAL_PATH = Path("/opt/airflow/data/eurostat_mineral_latest.csv")
TMP_GNB_PATH = Path("/opt/airflow/data/eurostat_gnb_latest.csv")


def fetch_eurostat_dataset(dataset_code, tmp_path):
    """Fetch an Eurostat dataset via the JSON statistics API.

    Parses the JSON-stat 1.0 response (logic delegated to
    lib.eurostat_parser), reshapes columns to match downstream SQL
    schemas, and writes a CSV to disk for COPY-based loading.
    """
    url = f"https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/{dataset_code}"
    params = {"format": "JSON", "lang": "en"}

    try:
        logging.info(f"Téléchargement Eurostat {dataset_code} via JSON API...")
        response = requests.get(url, params=params, timeout=120)
        response.raise_for_status()
        json_data = response.json()

        # --- PARSING DU JSON EUROSTAT ---
        # Logique de parsing extraite dans lib/eurostat_parser.py pour testabilité
        rows = parse_eurostat_json(json_data)
        df_long = pl.DataFrame(rows)

        # --- ADAPTATION AU FORMAT DE LA BASE DE DONNÉES ---
        if dataset_code == "aei_fm_usefert":
            df_long = df_long.select(
                [
                    pl.col("time").alias("annee").cast(pl.Int32),
                    pl.col("geo").alias("pays"),
                    pl.col("nutrient").alias("nutriment"),
                    pl.col("valeur").cast(pl.Float64),
                    pl.lit("KG_HA").alias("unite"),
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
                    pl.lit("KG_HA").alias("unite"),
                ]
            )

        df_long.write_csv(tmp_path)
        logging.info(f"{dataset_code}: {df_long.shape[0]} lignes sauvegardées.")

    except Exception as e:
        logging.error(f"Erreur fetch {dataset_code}: {e!s}")
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
