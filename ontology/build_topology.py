"""
build_topology.py

Step 3 of Hydrologic Knowledge Graph construction.

Creates the river network topology by linking Reach nodes
using the hydro:flowsTo object property.

Input:
    NHDPlus flowline dataset (shapefile or CSV)

Output:
    basin_topology.ttl
"""

import geopandas as gpd
import pandas as pd

from rdflib import Graph, Namespace
from rdflib.namespace import RDF


# ----------------------------------------------------------
# CONFIGURATION
# ----------------------------------------------------------

HYDRO = Namespace("http://example.org/hydro/ontology#")

INPUT_REACH_FILE = "../data/NHDPlus_flowlines.shp"

OUTPUT_TTL = "../rdf/basin_topology.ttl"


# ----------------------------------------------------------
# LOAD DATA
# ----------------------------------------------------------

def load_reach_dataset(file_path):

    if file_path.endswith(".shp"):
        gdf = gpd.read_file(file_path)
        df = pd.DataFrame(gdf.drop(columns="geometry"))

    elif file_path.endswith(".csv"):
        df = pd.read_csv(file_path)

    else:
        raise ValueError("Unsupported file format")

    return df


# ----------------------------------------------------------
# DETECT COLUMN NAMES
# ----------------------------------------------------------

def detect_topology_columns(df):

    reach_col = None
    downstream_col = None

    for c in df.columns:

        cl = c.lower()

        if cl in ["comid", "reach_id", "reachid", "id"]:
            reach_col = c

        if cl in ["tocomid", "downstream", "toid", "nextdownid"]:
            downstream_col = c

    if reach_col is None:
        raise ValueError("Could not find reach ID column")

    if downstream_col is None:
        raise ValueError("Could not find downstream column")

    return reach_col, downstream_col


# ----------------------------------------------------------
# BUILD TOPOLOGY GRAPH
# ----------------------------------------------------------

def build_topology(df):

    g = Graph()

    g.bind("hydro", HYDRO)

    reach_col, downstream_col = detect_topology_columns(df)

    print(f"Reach column: {reach_col}")
    print(f"Downstream column: {downstream_col}")

    count = 0

    for _, row in df.iterrows():

        reach_id = row[reach_col]
        downstream_id = row[downstream_col]

        if pd.isna(reach_id) or pd.isna(downstream_id):
            continue

        if downstream_id == 0:
            continue

        reach_uri = HYDRO[f"Reach_{int(reach_id)}"]
        downstream_uri = HYDRO[f"Reach_{int(downstream_id)}"]

        g.add((reach_uri, HYDRO.flowsTo, downstream_uri))

        count += 1

    print(f"Created {count} flowsTo relationships")

    return g


# ----------------------------------------------------------
# MAIN
# ----------------------------------------------------------

def main():

    print("Loading reach dataset...")

    df = load_reach_dataset(INPUT_REACH_FILE)

    print(f"Loaded {len(df)} reaches")

    print("Building topology graph...")

    g = build_topology(df)

    print("Writing TTL file...")

    g.serialize(destination=OUTPUT_TTL, format="turtle")

    print(f"Topology saved to {OUTPUT_TTL}")


# ----------------------------------------------------------
# RUN
# ----------------------------------------------------------

if __name__ == "__main__":

    main()