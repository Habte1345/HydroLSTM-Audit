from pathlib import Path
import numpy as np
import pandas as pd
from rdflib import Graph

# =========================================================
# Paths
# =========================================================
KG_FILE = Path("rdf/hydroKG.ttl")


# =========================================================
# Load KG
# =========================================================
def load_graph(path):
    print("\nLoading HydroKG...\n")

    if not path.exists():
        raise FileNotFoundError(f"HydroKG file not found: {path}")

    g = Graph()
    g.parse(str(path), format="turtle")

    print(f"Loaded {len(g):,} triples\n")
    return g


# =========================================================
# Extract observations
# =========================================================
def extract_observations(g):
    print("Extracting observations via SPARQL...")

    query = """
    PREFIX hydro: <http://example.org/hydro/ontology#>

    SELECT ?basin ?date ?qobs
    WHERE {
        ?obs a hydro:Observation ;
             hydro:forCatchment ?basin ;
             hydro:hasTimeStep ?date ;
             hydro:hasDischarge ?qobs .
    }
    """

    results = g.query(query)

    data = []
    for row in results:
        basin = str(row.basin).split("Catchment_")[-1]
        date = str(row.date).split("TimeStep_")[-1]

        data.append({
            "basin": basin,
            "date": pd.to_datetime(date),
            "qobs": float(row.qobs)
        })

    df = pd.DataFrame(data)
    print(f"Total observations: {len(df):,}\n")
    return df


# =========================================================
# Extract predictions
# =========================================================
def extract_predictions(g):
    print("Extracting predictions via SPARQL...")

    query = """
    PREFIX hydro: <http://example.org/hydro/ontology#>

    SELECT ?basin ?date ?qsim
    WHERE {
        ?pred a hydro:Prediction ;
              hydro:forBasin ?basin ;
              hydro:hasTime ?date ;
              hydro:hasStreamflow ?qsim .
    }
    """

    results = g.query(query)

    data = []
    for row in results:
        basin = str(row.basin).split("Basin_")[-1]
        date = str(row.date)[:10]

        data.append({
            "basin": basin,
            "date": pd.to_datetime(date),
            "qsim": float(row.qsim)
        })

    df = pd.DataFrame(data)
    print(f"Total predictions: {len(df):,}\n")
    return df


# =========================================================
# Merge
# =========================================================
def build_timeseries(g):
    obs = extract_observations(g)
    pred = extract_predictions(g)

    print("Merging observation and prediction time series...\n")

    df = pd.merge(obs, pred, on=["basin", "date"], how="inner")
    df = df.sort_values(["basin", "date"]).reset_index(drop=True)

    if df.empty:
        raise RuntimeError("Merge produced 0 paired records.")

    # Derived variables
    df["abs_err"] = (df["qsim"] - df["qobs"]).abs()
    df["rel_err"] = np.where(df["qobs"] > 0, df["abs_err"] / df["qobs"], np.nan)
    df["ratio"] = np.where(df["qobs"] > 0, df["qsim"] / df["qobs"], np.nan)

    print(f"Paired records: {len(df):,}\n")
    return df


# =========================================================
# HARD CONSTRAINTS
# =========================================================
def check_negative_sim_flow(df):
    v = df[df["qsim"] < 0].copy()
    v["constraint"] = "NEGATIVE_SIM_FLOW"
    return v


def check_nonfinite(df):
    v = df[~np.isfinite(df["qsim"])].copy()
    v["constraint"] = "NONFINITE_SIM_FLOW"
    return v


# =========================================================
# HYDROLOGICALLY MEANINGFUL CHECKS
# =========================================================
def check_extreme_ratio(df, qmin=1.0, low=0.2, high=5.0):
    valid = df[df["qobs"] >= qmin].copy()
    v = valid[(valid["ratio"] < low) | (valid["ratio"] > high)].copy()
    v["constraint"] = "EXTREME_RATIO"
    return v


def check_near_zero_flow(df, qobs_min=5.0, qsim_zero=0.05):
    v = df[(df["qobs"] >= qobs_min) & (df["qsim"] <= qsim_zero)].copy()
    v["constraint"] = "NEAR_ZERO_WHILE_OBS_FLOWING"
    return v


def check_high_relative_error(df, qmin=1.0, threshold=1.0):
    valid = df[df["qobs"] >= qmin].copy()
    v = valid[valid["rel_err"] > threshold].copy()
    v["constraint"] = "HIGH_REL_ERROR"
    return v


# =========================================================
# EVENT DETECTION
# =========================================================
def identify_events(df_basin, flow_thresh=5.0):
    wet = df_basin["qobs"] >= flow_thresh
    event_id = (wet != wet.shift()).cumsum()
    df_basin["event_id"] = np.where(wet, event_id, np.nan)
    return df_basin


# =========================================================
# EVENT CHECKS
# =========================================================
def check_peak_timing(df, flow_thresh=5.0, lag_days=2):
    rows = []

    for basin, g in df.groupby("basin"):
        g = identify_events(g.copy(), flow_thresh)

        for _, ev in g.dropna(subset=["event_id"]).groupby("event_id"):
            if len(ev) < 2:
                continue

            obs_peak = ev.loc[ev["qobs"].idxmax()]
            sim_peak = ev.loc[ev["qsim"].idxmax()]

            lag = abs((sim_peak["date"] - obs_peak["date"]).days)

            if lag > lag_days:
                row = obs_peak.copy()
                row["constraint"] = "PEAK_TIMING_ERROR"
                row["lag_days"] = lag
                rows.append(row)

    return pd.DataFrame(rows)


def check_peak_magnitude(df, flow_thresh=5.0):
    rows = []

    for basin, g in df.groupby("basin"):
        g = identify_events(g.copy(), flow_thresh)

        for _, ev in g.dropna(subset=["event_id"]).groupby("event_id"):
            if len(ev) < 2:
                continue

            qobs_peak = ev["qobs"].max()
            qsim_peak = ev["qsim"].max()

            if qobs_peak <= 0:
                continue

            ratio = qsim_peak / qobs_peak

            if ratio < 0.5 or ratio > 1.5:
                row = ev.loc[ev["qobs"].idxmax()].copy()
                row["constraint"] = "PEAK_MAGNITUDE_ERROR"
                row["peak_ratio"] = ratio
                rows.append(row)

    return pd.DataFrame(rows)


# =========================================================
# RUN AUDIT
# =========================================================
def run_audit():
    g = load_graph(KG_FILE)
    df = build_timeseries(g)

    violations = pd.concat([
        check_negative_sim_flow(df),
        check_nonfinite(df),
        check_extreme_ratio(df),
        check_near_zero_flow(df),
        check_high_relative_error(df),
        check_peak_timing(df),
        check_peak_magnitude(df)
    ], ignore_index=True)

    return df, violations


# =========================================================
# ENTRY
# =========================================================
if __name__ == "__main__":

    print("\nStarting HydroKG physical auditing\n")

    df, violations = run_audit()

    output_dir = Path("audit_results")
    output_dir.mkdir(exist_ok=True)

    violations.to_csv(output_dir / "physical_violations.csv", index=False)

    print("\nAudit finished")
    print(f"Total violations: {len(violations):,}")













# from pathlib import Path
# import pandas as pd
# from rdflib import Graph

# # ------------------------------------------------
# # Paths
# # ------------------------------------------------

# KG_FILE = Path("rdf/hydroKG.ttl")


# # ------------------------------------------------
# # Load KG
# # ------------------------------------------------

# def load_graph(path):

#     print("\nLoading HydroKG...\n")

#     if not path.exists():
#         raise FileNotFoundError(f"HydroKG file not found: {path}")

#     g = Graph()
#     g.parse(str(path), format="turtle")

#     print(f"Loaded {len(g):,} triples\n")

#     return g


# # ------------------------------------------------
# # Extract observations
# # ------------------------------------------------

# def extract_observations(g):

#     print("Extracting observations via SPARQL...")

#     query = """
#     PREFIX hydro: <http://example.org/hydro/ontology#>

#     SELECT ?basin ?date ?qobs
#     WHERE {
#         ?obs a hydro:Observation ;
#              hydro:forCatchment ?basin ;
#              hydro:hasTimeStep ?date ;
#              hydro:hasDischarge ?qobs .
#     }
#     """

#     results = g.query(query)

#     data = []

#     for row in results:

#         basin = str(row.basin).split("Catchment_")[-1]
#         date = str(row.date).split("TimeStep_")[-1]

#         data.append({
#             "basin": basin,
#             "date": date,
#             "qobs": float(row.qobs)
#         })

#     df = pd.DataFrame(data)

#     print(f"Total observations: {len(df):,}\n")

#     return df


# # ------------------------------------------------
# # Extract predictions
# # ------------------------------------------------

# def extract_predictions(g):

#     print("Extracting predictions via SPARQL...")

#     query = """
#     PREFIX hydro: <http://example.org/hydro/ontology#>

#     SELECT ?basin ?date ?qsim
#     WHERE {
#         ?pred a hydro:Prediction ;
#               hydro:forBasin ?basin ;
#               hydro:hasTime ?date ;
#               hydro:hasStreamflow ?qsim .
#     }
#     """

#     results = g.query(query)

#     data = []

#     for row in results:

#         basin = str(row.basin).split("Basin_")[-1]
#         date = str(row.date)[:10]

#         data.append({
#             "basin": basin,
#             "date": date,
#             "qsim": float(row.qsim)
#         })

#     df = pd.DataFrame(data)

#     print(f"Total predictions: {len(df):,}\n")

#     return df


# # ------------------------------------------------
# # Merge observations and predictions
# # ------------------------------------------------

# def build_timeseries(g):

#     obs = extract_observations(g)
#     pred = extract_predictions(g)

#     print("Merging observation and prediction time series...\n")

#     df = pd.merge(obs, pred, on=["basin", "date"], how="inner")

#     print(f"Paired records: {len(df):,}\n")

#     if df.empty:
#         raise RuntimeError("Merge produced 0 paired records.")

#     return df


# # ------------------------------------------------
# # Constraint 1 — Non-negative flow
# # ------------------------------------------------

# def check_non_negative(df):

#     print("Checking constraint: Non-negative streamflow")

#     violations = df[df["qsim"] < 0].copy()
#     violations["constraint"] = "NEGATIVE_FLOW"

#     print("Violations:", len(violations))

#     return violations


# # ------------------------------------------------
# # Constraint 2 — Large prediction error
# # ------------------------------------------------

# def check_prediction_error(df, threshold=20):

#     print("Checking constraint: Large prediction error")

#     violations = df[(df["qsim"] - df["qobs"]).abs() > threshold].copy()
#     violations["constraint"] = "LARGE_ERROR"

#     print("Violations:", len(violations))

#     return violations


# # ------------------------------------------------
# # Constraint 3 — Runoff ratio anomaly
# # ------------------------------------------------

# def check_extreme_runoff(df):

#     print("Checking constraint: Runoff ratio anomaly")

#     valid = df[df["qobs"] > 0].copy()

#     ratio = valid["qsim"] / valid["qobs"]

#     violations = valid[(ratio > 10) | (ratio < 0.1)].copy()
#     violations["constraint"] = "RUNOFF_RATIO_ANOMALY"

#     print("Violations:", len(violations))

#     return violations


# # ------------------------------------------------
# # Run auditing
# # ------------------------------------------------

# def run_audit():

#     g = load_graph(KG_FILE)

#     df = build_timeseries(g)

#     v1 = check_non_negative(df)
#     v2 = check_prediction_error(df)
#     v3 = check_extreme_runoff(df)

#     violations = pd.concat([v1, v2, v3], ignore_index=True)

#     return violations


# # ------------------------------------------------
# # Entry
# # ------------------------------------------------

# if __name__ == "__main__":

#     print("\nStarting HydroKG physical auditing\n")

#     violations = run_audit()

#     output_dir = Path("audit_results")
#     output_dir.mkdir(exist_ok=True)

#     out_file = output_dir / "physical_violations.csv"

#     violations.to_csv(out_file, index=False)

#     print("\nAudit finished")
#     print(f"Total violations: {len(violations):,}")
#     print("Results saved to:", out_file)
