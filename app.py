import streamlit as st
import pandas as pd
from pathlib import Path
import plotly.express as px

DATA_DIR = Path("data")

st.set_page_config(
    page_title="Meta-OGD Dashboard Kanton Zürich",
    layout="wide"
)

st.title("Meta-OGD Dashboard – Kanton Zürich")
st.caption("Keyword-Abdeckung & 5-Star-Reifegrad der Open Government Data")

# ======================================================
# Load data
# ======================================================
@st.cache_data
def load_data():
    kw = pd.read_parquet(DATA_DIR / "kw_metrics.parquet")
    pub_scores = pd.read_parquet(DATA_DIR / "publisher_scores.parquet")
    pub_score_dist = pd.read_parquet(DATA_DIR / "publisher_score_dist.parquet")
    kpis = pd.read_parquet(DATA_DIR / "kpis.parquet")
    return kw, pub_scores, pub_score_dist, kpis

try:
    kw, pub_scores, pub_score_dist, kpis = load_data()
except Exception as e:
    st.error("Daten nicht gefunden. Bitte zuerst pipeline.py ausführen.")
    st.stop()

# ======================================================
# KPIs
# ======================================================
k = kpis.iloc[0]

col1, col2, col3, col4 = st.columns(4)
col1.metric("Datasets", int(k["datasets_total"]))
col2.metric("Publisher", int(k["publishers_total"]))
col3.metric("Keywords", int(k["keywords_total"]))
col4.metric("Letzte Aktualisierung", str(k["last_modified_max"])[:10])

st.divider()

# ======================================================
# Sidebar filters
# ======================================================
st.sidebar.header("Filter")

top_n = st.sidebar.slider("Top-N Keywords", min_value=10, max_value=200, value=50, step=10)

metric_choice = st.sidebar.selectbox(
    "Keyword-Metrik",
    ["datasets_total", "datasets_new_12m", "datasets_upd_12m"],
    format_func=lambda x: {
        "datasets_total": "Total Datasets",
        "datasets_new_12m": "Neue Datasets (12M)",
        "datasets_upd_12m": "Aktualisierte Datasets (12M)"
    }[x]
)

st.sidebar.caption("Reifegrad-Mapping: 1=Excel · 2=CSV · 3=JSON · 4=Linked Data · 5=API")

# ======================================================
# Keyword heatmap
# ======================================================
st.subheader("Keyword-Abdeckung")

kw_top = kw.sort_values(metric_choice, ascending=False).head(top_n)

kw_melt = kw_top.melt(
    id_vars="keyword",
    value_vars=["datasets_total", "datasets_new_12m", "datasets_upd_12m"],
    var_name="metric",
    value_name="count"
)

metric_labels = {
    "datasets_total": "Total",
    "datasets_new_12m": "Neu (12M)",
    "datasets_upd_12m": "Aktualisiert (12M)"
}
kw_melt["metric_label"] = kw_melt["metric"].map(metric_labels)

fig_kw = px.imshow(
    kw_melt.pivot(index="keyword", columns="metric_label", values="count"),
    aspect="auto",
    color_continuous_scale="Blues",
    title="Keywords × Metrik"
)

fig_kw.update_layout(
    height=800,
    yaxis_title="Keyword",
    xaxis_title="Metrik"
)

st.plotly_chart(fig_kw, use_container_width=True)

st.caption("Interpretation: Wo gibt es besonders viele Daten, und wo passiert aktuell viel Neues?")

st.divider()

# ======================================================
# Keyword momentum ranking
# ======================================================
st.subheader("Keyword-Momentum (Neue Datasets in den letzten 12 Monaten)")

kw_new_top = kw.sort_values("datasets_new_12m", ascending=False).head(30)

fig_kw_new = px.bar(
    kw_new_top,
    x="datasets_new_12m",
    y="keyword",
    orientation="h",
    title="Top 30 Keywords nach neuen Datasets (12M)"
)

fig_kw_new.update_layout(
    height=700,
    yaxis_title="Keyword",
    xaxis_title="Neue Datasets (12M)"
)

st.plotly_chart(fig_kw_new, use_container_width=True)

st.divider()

# ======================================================
# Publisher maturity
# ======================================================
st.subheader("Reifegrad nach Organisation (5-Star-Modell)")

pub_scores["avg_score_rounded"] = pub_scores["avg_score"].round(2)

fig_pub_rank = px.bar(
    pub_scores.head(25),
    x="avg_score_rounded",
    y="publisher",
    orientation="h",
    title="Top 25 Organisationen nach durchschnittlichem Reifegrad",
)

fig_pub_rank.update_layout(
    height=800,
    yaxis_title="Organisation",
    xaxis_title="Ø Reifegrad (1–5)"
)

st.plotly_chart(fig_pub_rank, use_container_width=True)

st.divider()

# ======================================================
# Publisher × Score heatmap
# ======================================================
st.subheader("Organisation × Reifegrad-Stufe")

heat_pub = pub_score_dist.copy()
heat_pub["dataset_best_score"] = heat_pub["dataset_best_score"].astype(int)

heat_pivot = heat_pub.pivot(
    index="publisher",
    columns="dataset_best_score",
    values="datasets"
).fillna(0)

heat_pivot = heat_pivot.sort_values(heat_pivot.columns.tolist(), ascending=False)

fig_pub_heat = px.imshow(
    heat_pivot,
    aspect="auto",
    color_continuous_scale="YlGnBu",
    title="Organisation × Reifegrad-Stufe (Anzahl Datasets)"
)

fig_pub_heat.update_layout(
    height=900,
    xaxis_title="Reifegrad-Stufe",
    yaxis_title="Organisation"
)

st.plotly_chart(fig_pub_heat, use_container_width=True)

st.caption("Je weiter rechts und je dunkler, desto reifer die publizierten Datenformate.")

st.divider()

# ======================================================
# Raw tables (optional)
# ======================================================
with st.expander("Rohdaten anzeigen"):
    st.write("Keyword-Metriken")
    st.dataframe(kw, use_container_width=True)

    st.write("Publisher-Scores")
    st.dataframe(pub_scores, use_container_width=True)
