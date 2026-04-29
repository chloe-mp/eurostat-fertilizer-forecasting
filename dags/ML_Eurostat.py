from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import logging

default_args = {
    'owner': 'chloe',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='engrais_ml_eurostat',
    default_args=default_args,
    description='ML Eurostat : prévisions Prophet sur les engrais',
    schedule_interval='@weekly',
    start_date=datetime(2026, 1, 13),
    catchup=False,
    tags=['eurostat', 'engrais', 'ml', 'prophet'],
)

# ─────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────
DB_CONN_STR = "postgresql+psycopg2://airflow:airflow@postgres/airflow"
FORECAST_HORIZON_YEARS = 5
MIN_YEARS_REQUIRED = 10
MODELS_DIR = "/opt/airflow/models/prophet"  # Dossier de sérialisation des modèles


# ─────────────────────────────────────────
# CREATE OUTPUT TABLES
# ─────────────────────────────────────────
create_ml_tables = SQLExecuteQueryOperator(
    task_id='create_ml_tables',
    conn_id='postgres_default',
    sql="""
    -- Table des prévisions Prophet
    CREATE TABLE IF NOT EXISTS eurostat_predictions (
        annee           INTEGER,
        pays            TEXT,
        nutriment       TEXT,
        indicateur      TEXT,
        yhat            NUMERIC,
        yhat_lower      NUMERIC,
        yhat_upper      NUMERIC,
        is_forecast     BOOLEAN,
        is_negative     BOOLEAN DEFAULT FALSE,
        predicted_at    TIMESTAMP DEFAULT NOW(),
        PRIMARY KEY (annee, pays, nutriment, indicateur)
    );

    -- Ajout de la colonne is_negative si elle n'existe pas encore (migration)
    ALTER TABLE eurostat_predictions
        ADD COLUMN IF NOT EXISTS is_negative BOOLEAN DEFAULT FALSE;

    -- Table des métriques de qualité par série (MAE, RMSE, coverage)
    CREATE TABLE IF NOT EXISTS eurostat_model_metrics (
        pays            TEXT,
        nutriment       TEXT,
        indicateur      TEXT,
        mae             NUMERIC,
        rmse            NUMERIC,
        coverage        NUMERIC,
        n_points        INTEGER,
        trained_at      TIMESTAMP DEFAULT NOW(),
        PRIMARY KEY (pays, nutriment, indicateur)
    );

    -- Table de log des runs (monitoring global)
    CREATE TABLE IF NOT EXISTS eurostat_run_log (
        run_id              TEXT,
        run_at              TIMESTAMP DEFAULT NOW(),
        series_ok           INTEGER,
        series_skipped      INTEGER,
        series_failed       INTEGER,
        predictions_total   INTEGER,
        predictions_negative INTEGER,
        PRIMARY KEY (run_id)
    );
    """,
    autocommit=True,
    dag=dag,
)


# ─────────────────────────────────────────
# TASK 1 : PROPHET — Prévisions + sérialisation + métriques
# ─────────────────────────────────────────
def run_prophet_forecasts(db_conn_str, forecast_horizon, min_years, models_dir, **context):
    import os
    import pickle
    import pandas as pd
    from prophet import Prophet
    from prophet.diagnostics import cross_validation, performance_metrics
    from sqlalchemy import create_engine, text

    # ── Préparation ──────────────────────────────────────────────────────────
    os.makedirs(models_dir, exist_ok=True)
    engine = create_engine(db_conn_str)
    run_id = context["run_id"]

    series_ok = 0
    series_skipped = 0
    series_failed = 0
    all_predictions = []
    all_metrics = []

    # ── Chargement des données ───────────────────────────────────────────────
    df = pd.read_sql(
        """
        SELECT annee, pays, nutriment,
            valeur_mineral::FLOAT,
            valeur_bilan_inputs::FLOAT,
            valeur_bilan_outputs::FLOAT,
            valeur_bilan_net::FLOAT
        FROM eurostat_engrais_final
        ORDER BY annee
        """,
        engine,
    )

    target_columns = [
        ("valeur_mineral",       "mineral"),
        ("valeur_bilan_inputs",  "bilan_inputs"),
        ("valeur_bilan_outputs", "bilan_outputs"),
        ("valeur_bilan_net",     "bilan_net"),
    ]

    # ── Boucle principale par série ──────────────────────────────────────────
    for col, indicateur in target_columns:
        for (pays, nutriment), group in df.groupby(["pays", "nutriment"]):

            serie_label = f"{pays}/{nutriment}/{indicateur}"
            serie = group[["annee", col]].dropna().sort_values("annee")

            # — Vérification du nombre minimum de points —
            if len(serie) < min_years:
                logging.info(f"SKIP {serie_label}: seulement {len(serie)} points (min={min_years}).")
                series_skipped += 1
                continue

            df_prophet = serie.rename(columns={"annee": "ds", col: "y"})
            df_prophet["ds"] = pd.to_datetime(df_prophet["ds"], format="%Y")

            try:
                # — Entraînement —
                model = Prophet(
                    yearly_seasonality=False,
                    weekly_seasonality=False,
                    daily_seasonality=False,
                    interval_width=0.9,
                )
                model.fit(df_prophet)

                # — Sérialisation du modèle —
                model_path = os.path.join(
                    models_dir,
                    f"{pays}_{nutriment}_{indicateur}.pkl"
                )
                with open(model_path, "wb") as f:
                    pickle.dump(model, f)
                logging.info(f"Modèle sauvegardé : {model_path}")

                # — Prévision —
                future = model.make_future_dataframe(periods=forecast_horizon, freq="A")
                forecast = model.predict(future)
                last_historical_year = df_prophet["ds"].dt.year.max()

                for _, f_row in forecast.iterrows():
                    year = f_row["ds"].year
                    yhat = round(float(f_row["yhat"]), 4)
                    yhat_lower = round(float(f_row["yhat_lower"]), 4)
                    yhat_upper = round(float(f_row["yhat_upper"]), 4)
                    is_forecast = year > last_historical_year

                    # — Flag prédiction négative —
                    # bilan_net peut légitimement être négatif (déficit), on ne le flagge pas
                    cant_be_negative = indicateur in ("mineral", "bilan_inputs", "bilan_outputs")
                    is_negative = cant_be_negative and yhat < 0

                    if is_negative:
                        logging.warning(
                            f"NEGATIVE {serie_label} année={year} : yhat={yhat:.0f} — flaggé."
                        )

                    all_predictions.append({
                        "annee":       year,
                        "pays":        pays,
                        "nutriment":   nutriment,
                        "indicateur":  indicateur,
                        "yhat":        yhat,
                        "yhat_lower":  yhat_lower,
                        "yhat_upper":  yhat_upper,
                        "is_forecast": is_forecast,
                        "is_negative": is_negative,
                    })

                # — Cross-validation temporelle pour les métriques —
                # Seulement si on a assez de points (≥ 15) pour que la CV soit fiable
                if len(serie) >= 15:
                    try:
                        df_cv = cross_validation(
                            model,
                            initial="3650 days",
                            period="365 days",
                            horizon="365 days",
                            disable_tqdm=True,
                        )
                        df_perf = performance_metrics(df_cv)
                        last_row = df_perf.tail(1)

                        all_metrics.append({
                            "pays":       pays,
                            "nutriment":  nutriment,
                            "indicateur": indicateur,
                            "mae":        round(float(last_row["mae"].values[0]), 2),
                            "rmse":       round(float(last_row["rmse"].values[0]), 2),
                            "coverage":   round(float(last_row["coverage"].values[0]), 4),
                            "n_points":   len(serie),
                        })
                        logging.info(
                            f"CV {serie_label}: MAE={last_row['mae'].values[0]:.0f}, "
                            f"coverage={last_row['coverage'].values[0]:.2f}"
                        )
                    except Exception as e_cv:
                        logging.warning(f"CV échouée pour {serie_label}: {e_cv}")

                series_ok += 1

            except Exception as e:
                logging.error(f"ERREUR Prophet pour {serie_label}: {e}")
                series_failed += 1
                continue

    # ── Sauvegarde des prévisions en base ────────────────────────────────────
    if not all_predictions:
        logging.warning("Aucune prévision générée.")
        return

    df_predictions = pd.DataFrame(all_predictions)
    n_negative = int(df_predictions["is_negative"].sum())

    with engine.begin() as conn:
        for _, r in df_predictions.iterrows():
            conn.execute(
                text("""
                INSERT INTO eurostat_predictions
                    (annee, pays, nutriment, indicateur,
                     yhat, yhat_lower, yhat_upper, is_forecast, is_negative)
                VALUES
                    (:annee, :pays, :nutriment, :indicateur,
                     :yhat, :yhat_lower, :yhat_upper, :is_forecast, :is_negative)
                ON CONFLICT (annee, pays, nutriment, indicateur)
                DO UPDATE SET
                    yhat         = EXCLUDED.yhat,
                    yhat_lower   = EXCLUDED.yhat_lower,
                    yhat_upper   = EXCLUDED.yhat_upper,
                    is_forecast  = EXCLUDED.is_forecast,
                    is_negative  = EXCLUDED.is_negative,
                    predicted_at = NOW()
                """),
                r.to_dict()
            )

    logging.info(f"Prophet: {len(all_predictions)} prévisions sauvegardées ({n_negative} négatives flaggées).")

    # ── Clamp des valeurs négatives à 0 ──────────────────────────────────────
    # Les prédictions négatives sont physiquement impossibles pour mineral/
    # bilan_inputs/bilan_outputs — on les force à 0 après insertion.
    if n_negative > 0:
        with engine.begin() as conn:
            conn.execute(text("""
                UPDATE eurostat_predictions
                SET yhat       = 0,
                    yhat_lower = 0,
                    yhat_upper = GREATEST(yhat_upper, 0)
                WHERE is_negative = TRUE
            """))
        logging.info(f"{n_negative} prédictions négatives clampées à 0.")

    # ── Sauvegarde des métriques en base ─────────────────────────────────────
    if all_metrics:
        df_metrics = pd.DataFrame(all_metrics)
        with engine.begin() as conn:
            for _, r in df_metrics.iterrows():
                conn.execute(
                    text("""
                    INSERT INTO eurostat_model_metrics
                        (pays, nutriment, indicateur, mae, rmse, coverage, n_points)
                    VALUES
                        (:pays, :nutriment, :indicateur, :mae, :rmse, :coverage, :n_points)
                    ON CONFLICT (pays, nutriment, indicateur)
                    DO UPDATE SET
                        mae        = EXCLUDED.mae,
                        rmse       = EXCLUDED.rmse,
                        coverage   = EXCLUDED.coverage,
                        n_points   = EXCLUDED.n_points,
                        trained_at = NOW()
                    """),
                    r.to_dict()
                )
        logging.info(f"Métriques sauvegardées pour {len(all_metrics)} séries.")

    # ── Log du run global ────────────────────────────────────────────────────
    with engine.begin() as conn:
        conn.execute(
            text("""
            INSERT INTO eurostat_run_log
                (run_id, series_ok, series_skipped, series_failed,
                 predictions_total, predictions_negative)
            VALUES
                (:run_id, :series_ok, :series_skipped, :series_failed,
                 :predictions_total, :predictions_negative)
            ON CONFLICT (run_id)
            DO UPDATE SET
                series_ok            = EXCLUDED.series_ok,
                series_skipped       = EXCLUDED.series_skipped,
                series_failed        = EXCLUDED.series_failed,
                predictions_total    = EXCLUDED.predictions_total,
                predictions_negative = EXCLUDED.predictions_negative,
                run_at               = NOW()
            """),
            {
                "run_id":                run_id,
                "series_ok":             series_ok,
                "series_skipped":        series_skipped,
                "series_failed":         series_failed,
                "predictions_total":     len(all_predictions),
                "predictions_negative":  n_negative,
            }
        )

    logging.info(
        f"Run terminé — OK: {series_ok}, ignorées: {series_skipped}, "
        f"erreurs: {series_failed}, prévisions négatives: {n_negative}"
    )


prophet_task = PythonOperator(
    task_id='run_prophet_forecasts',
    python_callable=run_prophet_forecasts,
    op_kwargs={
        "db_conn_str":      DB_CONN_STR,
        "forecast_horizon": FORECAST_HORIZON_YEARS,
        "min_years":        MIN_YEARS_REQUIRED,
        "models_dir":       MODELS_DIR,
    },
    dag=dag,
)


# ─────────────────────────────────────────
# DAG DEPENDENCIES
# ─────────────────────────────────────────
create_ml_tables >> prophet_task