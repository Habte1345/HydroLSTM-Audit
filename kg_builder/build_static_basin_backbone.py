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

from pathlib import Path

import geopandas as gpd
import pandas as pd

from rdflib import Graph, Namespace, Literal
from rdflib.namespace import RDF, XSD


# ----------------------------------------------------------
# CONFIGURATION
# ----------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Namespace (must match your ontology schema)
HYDRO = Namespace("http://example.org/hydro/ontology#")

# Input data
INPUT_REACH_FILE = PROJECT_ROOT / "data" / "NHDPlus_flowlines.shp"

# Output RDF file
OUTPUT_TTL = PROJECT_ROOT / "rdf" / "basin_static.ttl"


# ----------------------------------------------------------
# LOAD DATA
# ----------------------------------------------------------

def load_reach_dataset(file_path):
    file_path = str(file_path)

    if file_path.endswith(".shp"):
        gdf = gpd.read_file(file_path)
        if "geometry" in gdf.columns:
            df = pd.DataFrame(gdf.drop(columns="geometry"))
        else:
            df = pd.DataFrame(gdf)

    elif file_path.endswith(".csv"):
        df = pd.read_csv(file_path)

    else:
        raise ValueError("Unsupported file format")

    return df


# ----------------------------------------------------------
# BUILD STATIC BACKBONE
# ----------------------------------------------------------

def build_static_backbone(
    df=None,
    input_reach_file=INPUT_REACH_FILE,
    output_ttl=OUTPUT_TTL,
    write_output=True,
):
    if df is None:
        print("Loading reach dataset...")
        df = load_reach_dataset(input_reach_file)
        print(f"Loaded {len(df)} reaches")

    g = Graph()
    g.bind("hydro", HYDRO)

    # Detect columns automatically
    reach_id_col = None
    length_col = None
    slope_col = None
    capacity_col = None

    for c in df.columns:
        cl = c.lower()

        if cl in ["comid", "reach_id", "reachid", "id", "nhdplusid"]:
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

    print("Building static basin backbone...")

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

    if write_output:
        output_ttl = Path(output_ttl)
        output_ttl.parent.mkdir(parents=True, exist_ok=True)

        print("Writing TTL file...")
        g.serialize(destination=str(output_ttl), format="turtle")
        print(f"Static basin backbone saved to {output_ttl}")

    return g


# ----------------------------------------------------------
# MAIN
# ----------------------------------------------------------

def main():
    build_static_backbone()


# ----------------------------------------------------------
# RUN
# ----------------------------------------------------------

if __name__ == "__main__":
    main()