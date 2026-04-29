"""
Dashboard Streamlit — Prévisions d'utilisation des engrais en Europe
Données : Eurostat | Modèle : Prophet
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from sqlalchemy import create_engine, text

# ── Configuration page ────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Engrais Europe — Prévisions",
    page_icon="🌱",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── CSS personnalisé ──────────────────────────────────────────────────────────
st.markdown("""
<style>
  @import url('https://fonts.googleapis.com/css2?family=DM+Serif+Display:ital@0;1&family=DM+Sans:wght@300;400;500&display=swap');

  html, body, [class*="css"] {
    font-family: 'DM Sans', sans-serif;
  }

  /* Fond général */
  .stApp {
    background-color: #F4F1EB;
  }

  /* Sidebar */
  section[data-testid="stSidebar"] {
    background-color: #1C3A2F;
    border-right: none;
  }
  section[data-testid="stSidebar"] * {
    color: #D4E8DC !important;
  }
  section[data-testid="stSidebar"] .stSelectbox label,
  section[data-testid="stSidebar"] .stMultiSelect label {
    color: #8BB89A !important;
    font-size: 0.75rem;
    text-transform: uppercase;
    letter-spacing: 0.1em;
  }

  /* Titres */
  h1 {
    font-family: 'DM Serif Display', serif;
    color: #1C3A2F;
    font-size: 2.6rem !important;
    line-height: 1.15;
    margin-bottom: 0 !important;
  }
  h2, h3 {
    font-family: 'DM Serif Display', serif;
    color: #1C3A2F;
  }

  /* Metric cards */
  [data-testid="metric-container"] {
    background: #FFFFFF;
    border: 1px solid #E2DDD3;
    border-radius: 12px;
    padding: 1.2rem 1.5rem;
    box-shadow: 0 1px 4px rgba(0,0,0,0.06);
  }
  [data-testid="metric-container"] label {
    color: #7A8C7E !important;
    font-size: 0.75rem !important;
    text-transform: uppercase;
    letter-spacing: 0.08em;
  }
  [data-testid="metric-container"] [data-testid="stMetricValue"] {
    color: #1C3A2F !important;
    font-size: 1.8rem !important;
    font-weight: 500;
  }

  /* Divider */
  hr {
    border-color: #D4CFC4;
    margin: 1.5rem 0;
  }

  /* Badges indicateur */
  .badge {
    display: inline-block;
    padding: 3px 10px;
    border-radius: 20px;
    font-size: 0.72rem;
    font-weight: 500;
    letter-spacing: 0.05em;
    text-transform: uppercase;
  }
  .badge-mineral   { background: #D6EDDF; color: #1C6B3A; }
  .badge-organique { background: #FDE8CC; color: #8B4A0E; }
  .badge-bilan     { background: #E0D9F5; color: #3D1E8B; }

  /* Info box */
  .info-box {
    background: #EAF4EE;
    border-left: 3px solid #2E7D52;
    border-radius: 0 8px 8px 0;
    padding: 0.8rem 1rem;
    font-size: 0.85rem;
    color: #1C3A2F;
    margin-bottom: 1rem;
  }

  /* Warning box */
  .warn-box {
    background: #FEF3E2;
    border-left: 3px solid #E07B00;
    border-radius: 0 8px 8px 0;
    padding: 0.8rem 1rem;
    font-size: 0.85rem;
    color: #5C3A00;
    margin-bottom: 1rem;
  }
</style>
""", unsafe_allow_html=True)

# ── Connexion DB ──────────────────────────────────────────────────────────────
@st.cache_resource
def get_engine():
    return create_engine(
        "postgresql+psycopg2://airflow:airflow@localhost:5432/airflow"
    )

@st.cache_data(ttl=300)
def load_data():
    engine = get_engine()
    with engine.connect() as conn:
        df = pd.read_sql(text("""
            SELECT annee, pays, nutriment, indicateur,
                   yhat, yhat_lower, yhat_upper,
                   is_forecast, is_negative
            FROM eurostat_predictions
            ORDER BY pays, nutriment, indicateur, annee
        """), conn)
    return df

@st.cache_data(ttl=300)
def load_metrics():
    engine = get_engine()
    with engine.connect() as conn:
        df = pd.read_sql(text("""
            SELECT pays, nutriment, indicateur, mae, rmse, coverage, n_points
            FROM eurostat_model_metrics
            ORDER BY mae ASC
        """), conn)
    return df

@st.cache_data(ttl=300)
def load_run_log():
    engine = get_engine()
    with engine.connect() as conn:
        df = pd.read_sql(text("""
            SELECT * FROM eurostat_run_log ORDER BY run_at DESC LIMIT 1
        """), conn)
    return df

# ── Chargement des données ────────────────────────────────────────────────────
try:
    df = load_data()
    df_metrics = load_metrics()
    df_run = load_run_log()
    data_ok = True
except Exception as e:
    data_ok = False
    st.error(f"Connexion à la base impossible : {e}")
    st.info("Vérifiez que PostgreSQL est accessible sur localhost:5432")
    st.stop()

# ── Labels lisibles ───────────────────────────────────────────────────────────
NUTRIMENT_LABELS = {"N": "Azote (N)", "P": "Phosphore (P)"}
INDICATEUR_LABELS = {
    "mineral":       "Engrais minéraux",
    "organique":     "Engrais organiques",
    "bilan_inputs":  "Bilan — Entrées",
    "bilan_outputs": "Bilan — Sorties",
    "bilan_net":     "Bilan net",
}
PAYS_EU = {
    "AT": "Autriche", "BE": "Belgique", "BG": "Bulgarie", "CY": "Chypre",
    "CZ": "Tchéquie", "DE": "Allemagne", "DK": "Danemark", "EE": "Estonie",
    "EL": "Grèce", "ES": "Espagne", "FI": "Finlande", "FR": "France",
    "HR": "Croatie", "HU": "Hongrie", "IE": "Irlande", "IT": "Italie",
    "LT": "Lituanie", "LU": "Luxembourg", "LV": "Lettonie", "MT": "Malte",
    "NL": "Pays-Bas", "PL": "Pologne", "PT": "Portugal", "RO": "Roumanie",
    "SE": "Suède", "SI": "Slovénie", "SK": "Slovaquie",
    "NO": "Norvège", "CH": "Suisse", "UK": "Royaume-Uni",
}

def pays_label(code):
    return PAYS_EU.get(code, code)

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🌱 Filtres")
    st.markdown("---")

    pays_dispo = sorted(df["pays"].unique())
    # Garder uniquement les pays nationaux (codes 2 lettres)
    pays_nationaux = [p for p in pays_dispo if len(p) == 2]

    pays_options = {pays_label(p): p for p in pays_nationaux}
    pays_selected_label = st.selectbox(
        "Pays",
        options=list(pays_options.keys()),
        index=list(pays_options.keys()).index("France") if "France" in pays_options else 0,
    )
    pays_selected = pays_options[pays_selected_label]

    nutriment_selected = st.selectbox(
        "Nutriment",
        options=list(NUTRIMENT_LABELS.keys()),
        format_func=lambda x: NUTRIMENT_LABELS[x],
    )

    indicateurs_dispo = sorted(df[
        (df["pays"] == pays_selected) & (df["nutriment"] == nutriment_selected)
    ]["indicateur"].unique())

    indicateur_selected = st.selectbox(
        "Indicateur",
        options=indicateurs_dispo,
        format_func=lambda x: INDICATEUR_LABELS.get(x, x),
    )

    st.markdown("---")
    show_ci = st.toggle("Intervalle de confiance", value=True)
    show_metrics = st.toggle("Métriques du modèle", value=True)

    st.markdown("---")
    st.markdown(
        "<div style='font-size:0.72rem; color:#8BB89A; text-transform:uppercase; letter-spacing:0.08em'>Source</div>"
        "<div style='font-size:0.82rem; margin-top:4px'>Eurostat — Nutrient budgets<br>Modèle Prophet (Meta)</div>",
        unsafe_allow_html=True,
    )

# ── En-tête ───────────────────────────────────────────────────────────────────
col_title, col_run = st.columns([3, 1])
with col_title:
    st.markdown(
        f"<h1>Engrais en Europe</h1>"
        f"<p style='color:#5A7A65; font-size:1.05rem; margin-top:4px;'>"
        f"Prévisions 2024–2028 · {INDICATEUR_LABELS.get(indicateur_selected, indicateur_selected)} · "
        f"{NUTRIMENT_LABELS.get(nutriment_selected, nutriment_selected)}</p>",
        unsafe_allow_html=True,
    )
with col_run:
    if not df_run.empty:
        run = df_run.iloc[0]
        run_date = pd.to_datetime(run["run_at"]).strftime("%d/%m/%Y %H:%M")
        st.markdown(
            f"<div style='text-align:right; padding-top:1rem;'>"
            f"<div style='font-size:0.72rem; color:#7A8C7E; text-transform:uppercase; letter-spacing:0.08em'>Dernier calcul</div>"
            f"<div style='font-size:0.9rem; color:#1C3A2F; font-weight:500'>{run_date}</div>"
            f"<div style='font-size:0.78rem; color:#7A8C7E'>{run['series_ok']} séries modélisées</div>"
            f"</div>",
            unsafe_allow_html=True,
        )

st.markdown("<hr>", unsafe_allow_html=True)

# ── Filtrage ──────────────────────────────────────────────────────────────────
df_serie = df[
    (df["pays"] == pays_selected) &
    (df["nutriment"] == nutriment_selected) &
    (df["indicateur"] == indicateur_selected)
].copy()

df_hist = df_serie[~df_serie["is_forecast"]].sort_values("annee")
df_prev = df_serie[df_serie["is_forecast"]].sort_values("annee")

# ── KPIs ──────────────────────────────────────────────────────────────────────
if not df_prev.empty and not df_hist.empty:
    val_last_hist = df_hist["yhat"].iloc[-1]
    val_last_prev = df_prev["yhat"].iloc[-1]
    annee_last_prev = int(df_prev["annee"].iloc[-1])
    delta_pct = ((val_last_prev - val_last_hist) / abs(val_last_hist) * 100) if val_last_hist != 0 else 0
    has_negative = df_prev["is_negative"].any()

    # Métriques modèle pour cette série
    met_row = df_metrics[
        (df_metrics["pays"] == pays_selected) &
        (df_metrics["nutriment"] == nutriment_selected) &
        (df_metrics["indicateur"] == indicateur_selected)
    ]

    k1, k2, k3, k4 = st.columns(4)
    with k1:
        st.metric(
            "Dernière valeur historique",
            f"{val_last_hist:,.0f} t",
            delta=None,
        )
    with k2:
        st.metric(
            f"Prévision {annee_last_prev}",
            f"{val_last_prev:,.0f} t",
            delta=f"{delta_pct:+.1f}% vs historique",
            delta_color="normal",
        )
    with k3:
        if not met_row.empty:
            st.metric("MAE (erreur moy.)", f"{met_row['mae'].values[0]:,.0f} t")
        else:
            st.metric("MAE (erreur moy.)", "—")
    with k4:
        if not met_row.empty:
            cov = met_row["coverage"].values[0]
            st.metric("Couverture IC 80%", f"{cov*100:.0f}%")
        else:
            st.metric("Couverture IC 80%", "—")

    st.markdown("")

    # Alerte prédictions négatives
    if has_negative:
        st.markdown(
            "<div class='warn-box'>⚠️ Certaines prévisions pour cette série atteignaient des valeurs négatives "
            "(physiquement impossibles) — elles ont été ramenées à 0 automatiquement.</div>",
            unsafe_allow_html=True,
        )

# ── Graphique principal ───────────────────────────────────────────────────────
fig = go.Figure()

# Intervalle de confiance (prévisions)
if show_ci and not df_prev.empty:
    fig.add_trace(go.Scatter(
        x=pd.concat([df_prev["annee"], df_prev["annee"][::-1]]),
        y=pd.concat([df_prev["yhat_upper"], df_prev["yhat_lower"][::-1]]),
        fill="toself",
        fillcolor="rgba(46, 125, 82, 0.12)",
        line=dict(color="rgba(0,0,0,0)"),
        name="Intervalle de confiance 80%",
        hoverinfo="skip",
    ))

# Intervalle de confiance (historique)
if show_ci and not df_hist.empty:
    fig.add_trace(go.Scatter(
        x=pd.concat([df_hist["annee"], df_hist["annee"][::-1]]),
        y=pd.concat([df_hist["yhat_upper"], df_hist["yhat_lower"][::-1]]),
        fill="toself",
        fillcolor="rgba(100, 100, 100, 0.07)",
        line=dict(color="rgba(0,0,0,0)"),
        name="Incertitude historique",
        hoverinfo="skip",
    ))

# Ligne historique
if not df_hist.empty:
    fig.add_trace(go.Scatter(
        x=df_hist["annee"],
        y=df_hist["yhat"],
        mode="lines+markers",
        name="Données historiques",
        line=dict(color="#1C3A2F", width=2.5),
        marker=dict(size=6, color="#1C3A2F"),
        hovertemplate="<b>%{x}</b><br>%{y:,.0f} tonnes<extra>Historique</extra>",
    ))

# Ligne de prévision
if not df_prev.empty:
    # Raccord visuel avec le dernier point historique
    if not df_hist.empty:
        raccord_x = [df_hist["annee"].iloc[-1]] + list(df_prev["annee"])
        raccord_y = [df_hist["yhat"].iloc[-1]] + list(df_prev["yhat"])
    else:
        raccord_x = list(df_prev["annee"])
        raccord_y = list(df_prev["yhat"])

    fig.add_trace(go.Scatter(
        x=raccord_x,
        y=raccord_y,
        mode="lines+markers",
        name="Prévisions Prophet",
        line=dict(color="#2E7D52", width=2.5, dash="dot"),
        marker=dict(size=7, color="#2E7D52", symbol="diamond"),
        hovertemplate="<b>%{x}</b><br>%{y:,.0f} tonnes<extra>Prévision</extra>",
    ))

# Ligne verticale séparation historique/prévision
if not df_hist.empty and not df_prev.empty:
    annee_pivot = df_hist["annee"].max()
    fig.add_vline(
        x=annee_pivot,
        line_dash="dash",
        line_color="#C4B99A",
        line_width=1.5,
        annotation_text="Début prévision",
        annotation_position="top right",
        annotation_font_color="#7A6A50",
        annotation_font_size=11,
    )

fig.update_layout(
    plot_bgcolor="#FFFFFF",
    paper_bgcolor="#F4F1EB",
    font=dict(family="DM Sans", color="#3A4A3E"),
    height=460,
    margin=dict(l=20, r=20, t=30, b=20),
    legend=dict(
        orientation="h",
        yanchor="bottom",
        y=1.02,
        xanchor="left",
        x=0,
        bgcolor="rgba(0,0,0,0)",
        font=dict(size=12),
    ),
    xaxis=dict(
        title="",
        gridcolor="#EAE6DE",
        tickfont=dict(size=12),
        showline=True,
        linecolor="#D4CFC4",
    ),
    yaxis=dict(
        title="Tonnes de nutriment pur",
        gridcolor="#EAE6DE",
        tickfont=dict(size=12),
        showline=False,
        tickformat=",",
    ),
    hovermode="x unified",
)

st.plotly_chart(fig, use_container_width=True)

# ── Section métriques détaillées ──────────────────────────────────────────────
if show_metrics and not df_metrics.empty:
    st.markdown("### Qualité des modèles — Top 10 séries")

    col_info, _ = st.columns([3, 1])
    with col_info:
        st.markdown(
            "<div class='info-box'>Le MAE (Mean Absolute Error) mesure l'erreur moyenne en tonnes. "
            "La couverture indique la proportion de valeurs réelles tombant dans l'intervalle de confiance prévu à 80%.</div>",
            unsafe_allow_html=True,
        )

    df_top = df_metrics[df_metrics["pays"].str.len() == 2].head(10).copy()
    df_top["Pays"] = df_top["pays"].apply(pays_label)
    df_top["Nutriment"] = df_top["nutriment"].map(NUTRIMENT_LABELS)
    df_top["Indicateur"] = df_top["indicateur"].map(INDICATEUR_LABELS)
    df_top["MAE (t)"] = df_top["mae"].apply(lambda x: f"{x:,.0f}")
    df_top["RMSE (t)"] = df_top["rmse"].apply(lambda x: f"{x:,.0f}")
    df_top["Couverture"] = df_top["coverage"].apply(lambda x: f"{x*100:.0f}%")
    df_top["N points"] = df_top["n_points"]

    st.dataframe(
        df_top[["Pays", "Nutriment", "Indicateur", "MAE (t)", "RMSE (t)", "Couverture", "N points"]],
        use_container_width=True,
        hide_index=True,
    )

# ── Footer ────────────────────────────────────────────────────────────────────
st.markdown("<hr>", unsafe_allow_html=True)
st.markdown(
    "<div style='font-size:0.78rem; color:#9A9488; text-align:center;'>"
    "Données Eurostat · Modèle Prophet (Meta) · Pipeline Apache Airflow"
    "</div>",
    unsafe_allow_html=True,
)