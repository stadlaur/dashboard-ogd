import pandas as pd
import requests
from datetime import datetime, timedelta, timezone
from pathlib import Path

URL = "https://www.web.statistik.zh.ch/ogd/daten/zhweb.json"
OUTDIR = Path("data")
OUTDIR.mkdir(exist_ok=True)

def parse_dt(s):
    return pd.to_datetime(s, errors="coerce")

def _s(x) -> str:
    """Convert possibly-NaN values to lowercase string safely."""
    if x is None or (isinstance(x, float) and pd.isna(x)) or pd.isna(x):
        return ""
    return str(x).strip().lower()

import pandas as pd  # muss im File sowieso schon da sein; falls doppelt: ok

def _s(x) -> str:
    """Convert possibly-NaN values to lowercase string safely."""
    try:
        # pandas NA / NaN
        if pd.isna(x):
            return ""
    except Exception:
        pass
    return str(x).strip().lower()

def score_distribution(meta: dict) -> int:
    fmt = _s(meta.get("format"))
    media = _s(meta.get("mediaType"))
    access = _s(meta.get("accessUrl"))
    download = _s(meta.get("downloadUrl"))

    u = " ".join([fmt, media, access, download])

    api_tokens = [
        "api", "odata", "sparql", "wfs", "wms",
        "arcgis/rest", "service=wfs", "service=wms", "/rest", "rest/"
    ]
    if any(t in u for t in api_tokens):
        return 5

    if any(t in u for t in ["rdf", "turtle", "ttl", "json-ld", "n-triples"]):
        return 4

    if any(t in u for t in ["geojson", "json", "application/json", "xml"]):
        return 3

    if "csv" in u or "text/csv" in u:
        return 2

    if any(t in u for t in ["xls", "xlsx", "excel", "spreadsheetml"]):
        return 1

    return 2


def main():
    print("Loading metadata from:", URL)
    raw = requests.get(URL, timeout=60).json()

    ds = pd.json_normalize(raw["dataset"])

    # --- Dates ---
    ds["issued_dt"] = ds["issued"].apply(parse_dt)
    ds["modified_dt"] = ds["modified"].apply(parse_dt)

    now = datetime.now(timezone.utc)
    cutoff = pd.Timestamp(now - timedelta(days=365)).tz_convert(None)

    # ======================================================
    # Keyword metrics
    # ======================================================
    kw = ds[["identifier", "keyword", "issued_dt", "modified_dt"]].explode("keyword")

    kw["is_new_12m"] = kw["issued_dt"] >= cutoff
    kw["is_upd_12m"] = kw["modified_dt"] >= cutoff

    kw_metrics = (
        kw.groupby("keyword", dropna=True)
          .agg(
              datasets_total=("identifier", "nunique"),
              datasets_new_12m=("is_new_12m", "sum"),
              datasets_upd_12m=("is_upd_12m", "sum"),
          )
          .reset_index()
          .sort_values("datasets_total", ascending=False)
    )

    kw_metrics.to_parquet(OUTDIR / "kw_metrics.parquet", index=False)

    # ======================================================
    # Publisher maturity (5-star)
    # ======================================================
    pub = ds[["identifier", "publisher"]].explode("publisher")

    dist = ds[["identifier", "distribution"]].explode("distribution")
    dist_norm = pd.json_normalize(dist["distribution"]).add_prefix("dist_")
    dist_rows = pd.concat(
        [dist[["identifier"]].reset_index(drop=True), dist_norm],
        axis=1
    )

    dist_rows["dist_score"] = dist_rows.apply(
        lambda r: score_distribution({
            "format": r.get("dist_format"),
            "mediaType": r.get("dist_mediaType"),
            "accessUrl": r.get("dist_accessUrl"),
            "downloadUrl": r.get("dist_downloadUrl"),
        }),
        axis=1
    )

    # Best score per dataset
    ds_best = (
        dist_rows.groupby("identifier")["dist_score"]
        .max()
        .reset_index(name="dataset_best_score")
    )

    pub_scores = (
        pub.merge(ds_best, on="identifier", how="left")
           .groupby("publisher", dropna=True)
           .agg(
               datasets=("identifier", "nunique"),
               avg_score=("dataset_best_score", "mean"),
               med_score=("dataset_best_score", "median"),
           )
           .reset_index()
           .sort_values(["avg_score", "datasets"], ascending=False)
    )

    pub_scores.to_parquet(OUTDIR / "publisher_scores.parquet", index=False)

    pub_score_dist = (
        pub.merge(ds_best, on="identifier", how="left")
           .groupby(["publisher", "dataset_best_score"], dropna=True)["identifier"]
           .nunique()
           .reset_index(name="datasets")
    )

    pub_score_dist.to_parquet(OUTDIR / "publisher_score_dist.parquet", index=False)

    # ======================================================
    # Metadata snapshot (for KPIs)
    # ======================================================
    kpis = {
        "datasets_total": int(ds["identifier"].nunique()),
        "publishers_total": int(pub["publisher"].nunique()),
        "keywords_total": int(kw["keyword"].nunique()),
        "last_modified_max": str(ds["modified_dt"].max()),
        "run_timestamp": str(now),
    }

    pd.DataFrame([kpis]).to_parquet(OUTDIR / "kpis.parquet", index=False)

    print("Pipeline finished. Files written to data/")

if __name__ == "__main__":
    main()
