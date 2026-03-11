"""
build_static_basin_backbone.py

Step 2 of Hydrologic Knowledge Graph construction.

Creates static hydrologic entities (Reach nodes) and attaches
their permanent attributes (length, slope, etc).

Input:
    NHDPlus flowlines (shapefile or CSV)

Output:
    basin_static.ttl
"""

import geopandas as gpd
import pandas as pd

from rdflib import Graph, Namespace, Literal
from rdflib.namespace import RDF, XSD


# ----------------------------------------------------------
# CONFIGURATION
# ----------------------------------------------------------

# Namespace (must match your ontology schema)
HYDRO = Namespace("http://example.org/hydro/ontology#")

# Input data
INPUT_REACH_FILE = "../data/NHDPlus_flowlines.shp"

# Output RDF file
OUTPUT_TTL = "../rdf/basin_static.ttl"


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
# BUILD STATIC BACKBONE
# ----------------------------------------------------------

def build_static_backbone(df):

    g = Graph()

    g.bind("hydro", HYDRO)

    # Detect columns automatically
    reach_id_col = None
    length_col = None
    slope_col = None
    capacity_col = None

    for c in df.columns:

        cl = c.lower()

        if cl in ["comid", "reach_id", "reachid", "id"]:
            reach_id_col = c

        if "length" in cl:
            length_col = c

        if "slope" in cl:
            slope_col = c

        if "capacity" in cl:
            capacity_col = c


    if reach_id_col is None:
        raise ValueError("No reach ID column found")


    # ------------------------------------------------------
    # CREATE REACH INSTANCES
    # ------------------------------------------------------

    for _, row in df.iterrows():

        reach_id = str(row[reach_id_col])

        reach_uri = HYDRO[f"Reach_{reach_id}"]

        # Declare class
        g.add((reach_uri, RDF.type, HYDRO.Reach))

        # Attach attributes
        if length_col and pd.notna(row[length_col]):

            g.add((
                reach_uri,
                HYDRO.hasReachLength,
                Literal(float(row[length_col]), datatype=XSD.float)
            ))

        if slope_col and pd.notna(row[slope_col]):

            g.add((
                reach_uri,
                HYDRO.hasSlope,
                Literal(float(row[slope_col]), datatype=XSD.float)
            ))

        if capacity_col and pd.notna(row[capacity_col]):

            g.add((
                reach_uri,
                HYDRO.hasMaxCapacity,
                Literal(float(row[capacity_col]), datatype=XSD.float)
            ))

    return g


# ----------------------------------------------------------
# MAIN
# ----------------------------------------------------------

def main():

    print("Loading reach dataset...")

    df = load_reach_dataset(INPUT_REACH_FILE)

    print(f"Loaded {len(df)} reaches")

    print("Building static basin backbone...")

    g = build_static_backbone(df)

    print("Writing TTL file...")

    g.serialize(destination=OUTPUT_TTL, format="turtle")

    print(f"Static basin backbone saved to {OUTPUT_TTL}")


# ----------------------------------------------------------
# RUN
# ----------------------------------------------------------

if __name__ == "__main__":

    main()