
# ==========================================================
# Hydro Schema Dashboard (T-Box + A-Box Builder + Visualizer)
#   Step 1: Build schema (T-Box) from glossaries.json -> hydro_schema.ttl
#   Step 2: Build static basin backbone (A-Box static) from reach CSV -> basin_<gauge>.ttl
#   Step 3: Build topology (A-Box topology) from topology CSV -> basin_<gauge>_topology.ttl
# Then: Visualize (schema + abox files) in a KG explorer + basin map.
# ==========================================================

import os
import glob
import json
from pathlib import Path
from typing import Optional, Tuple, Dict, List
import networkx as nx
import plotly.graph_objects as go

import networkx as nx
import plotly.graph_objects as go
import rdflib

import streamlit as st
import pandas as pd
import geopandas as gpd

import folium
from streamlit_folium import st_folium

import rdflib
from rdflib import Graph, Namespace, Literal, RDF, RDFS, OWL, URIRef
from rdflib.namespace import XSD

from pyvis.network import Network
import streamlit.components.v1 as components


# ==========================================================
# STREAMLIT CONFIG
# ==========================================================
st.set_page_config(
    page_title="Hydro Schema Dashboard (T-Box / A-Box Builder)",
    page_icon="🌊",
    layout="wide"
)

# ==========================================================
# PATHS / SETTINGS
# ==========================================================
BASE_DIR = "F:/Github_repos/HydroLSTM-Audit"

ASSETS_DIR = os.path.join(BASE_DIR, "assets")
DATA_DIR = os.path.join(BASE_DIR, "hydro_data")

# Output structure (keeps T-Box and A-Box separate)
OUTPUT_DIR = os.path.join(BASE_DIR, "outputs")
SCHEMA_DIR = os.path.join(OUTPUT_DIR, "schema")
ABOX_STATIC_DIR = os.path.join(OUTPUT_DIR, "abox_static")
ABOX_TOPO_DIR = os.path.join(OUTPUT_DIR, "abox_topology")
VIZ_DIR = os.path.join(OUTPUT_DIR, "viz")

for d in [OUTPUT_DIR, SCHEMA_DIR, ABOX_STATIC_DIR, ABOX_TOPO_DIR, VIZ_DIR]:
    os.makedirs(d, exist_ok=True)

# Basin polygons + hydro layers (as in your app)
BASIN_POLYGONS_FILE = os.path.join(
    BASE_DIR,
    "data/camels/camels_basin_shapes.shp"
)

NHD_FLOWLINES_FILE = os.path.join(BASE_DIR, "hydro_data/NHDPlus_flowlines.shp")
USGS_GAUGES_FILE = os.path.join(BASE_DIR, "hydro_data/usgs_gauges.shp")

BASIN_ID_FIELD = "gauge_id"

LOGO1 = os.path.join(ASSETS_DIR, "CIROHLogo_200x200.png")
LOGO2 = os.path.join(BASE_DIR, "NFDI4Earth_logo.png")


DEFAULT_HYDRO_NS = "https://alabama-hydro.org/ontology/"

# KG visualization safety
MAX_TRIPLES_TO_DRAW = 500
MAX_NODES_TO_DRAW = 200

LOGO3 = r"C:\Users\hdagne1\Downloads\NRT_Logo_Primary-Logo-Wide-1024x287.png"
# ==========================================================
# UI HEADER
# ==========================================================
# ==========================================================
# UI HEADER
# ==========================================================
LOGO4 = r"C:\Users\hdagne1\Downloads\ciroh_logo-f459844bc9ac125dfe0e1ba7dee193d4.png"
LOGO5 = r"C:\Users\hdagne1\Downloads\images.jpg"

def render_header():
    col1, col2, col3, col4, col5, col6 = st.columns([1.2, 5, 2, 2, 2, 2])

    with col1:
        if os.path.exists(LOGO1):
            st.image(LOGO1, width=120)

    with col2:
        st.markdown(
            """
            <h1 style="text-align:center;color:black;font-size:54px;margin-bottom:16px">
            Hydro-Secure Demonstration Dashboard
            </h1>

            <p style="text-align:center;font-size:px;margin-top:5px;color:blue;">
            Team Members: Habtamu Tamiru (PhD Candidate) | AJ Wood (MSc) | Basit Akinade (PhD Student) <br>
            Instructors: Dr. Steven Burian | Abby Davies (MSc) | Dr. Hannah Holcomb <br>
            Team Leaders: Dr. Jiaqi Gong | Dr. Shenglin Li | Dr. Travis Loof
            </p>
            """,
            unsafe_allow_html=True
        )

    with col3:
        if os.path.exists(LOGO2):
            st.image(LOGO2, width=300)

    with col4:
        if os.path.exists(LOGO3):
            st.image(LOGO3, width=260)

    with col5:
        if os.path.exists(LOGO4):
            st.image(LOGO4, width=180)

    with col6:
        if os.path.exists(LOGO5):
            st.image(LOGO5, width=150)

    st.divider()

# ==========================================================
# SMALL HELPERS
# ==========================================================
def uri_label(uri: str) -> str:
    uri = str(uri)
    if "#" in uri:
        return uri.split("#")[-1]
    if "/" in uri:
        return uri.rstrip("/").split("/")[-1]
    return uri


def safe_read_json(path: str) -> dict:
    with open(path, "r") as f:
        return json.load(f)


def save_uploaded_file(uploaded, dst_dir: str) -> Optional[str]:
    if uploaded is None:
        return None
    os.makedirs(dst_dir, exist_ok=True)
    dst_path = os.path.join(dst_dir, uploaded.name)
    with open(dst_path, "wb") as f:
        f.write(uploaded.getbuffer())
    return dst_path


def ensure_str(x) -> str:
    if pd.isna(x):
        return ""
    return str(x)


def detect_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    cols = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in cols:
            return cols[cand.lower()]
    return None


def to_float_or_none(x):
    try:
        if pd.isna(x):
            return None
        return float(x)
    except Exception:
        return None


# ==========================================================
# GEO DATA LOADERS
# ==========================================================
@st.cache_data
def load_basins() -> gpd.GeoDataFrame:
    gdf = gpd.read_file(BASIN_POLYGONS_FILE)
    gdf[BASIN_ID_FIELD] = gdf[BASIN_ID_FIELD].astype(str)
    if gdf.crs and gdf.crs.to_string() != "EPSG:4326":
        gdf = gdf.to_crs(4326)
    return gdf


@st.cache_data
def load_flowlines() -> Optional[gpd.GeoDataFrame]:
    if not os.path.exists(NHD_FLOWLINES_FILE):
        return None
    gdf = gpd.read_file(NHD_FLOWLINES_FILE)
    if gdf.crs and gdf.crs.to_string() != "EPSG:4326":
        gdf = gdf.to_crs(4326)
    return gdf


@st.cache_data
def load_gauges() -> Optional[gpd.GeoDataFrame]:
    if not os.path.exists(USGS_GAUGES_FILE):
        return None
    gdf = gpd.read_file(USGS_GAUGES_FILE)
    if gdf.crs and gdf.crs.to_string() != "EPSG:4326":
        gdf = gdf.to_crs(4326)
    return gdf


# ==========================================================
# STEP 1: BUILD ONTOLOGY SCHEMA (T-BOX) FROM glossaries.json

# ==========================================================
def build_schema_from_glossaries(glossaries_json_path: str, hydro_ns: str, output_ttl_path: str) -> Tuple[str, Dict[str, int]]:
    data = safe_read_json(glossaries_json_path)

    g = Graph()
    HYDRO = Namespace(hydro_ns)

    g.bind("hydro", HYDRO)
    g.bind("owl", OWL)
    g.bind("rdfs", RDFS)
    g.bind("xsd", XSD)

    ontology_uri = URIRef(hydro_ns.rstrip("/").rstrip("#"))
    g.add((ontology_uri, RDF.type, OWL.Ontology))

    desc = data.get("metadata", {}).get("description", "")
    if desc:
        g.add((ontology_uri, RDFS.comment, Literal(desc)))

    # Optional: declare annotation properties used in the JSON (if any)
    g.add((HYDRO.auditRole, RDF.type, OWL.AnnotationProperty))
    g.add((HYDRO.hasUnit, RDF.type, OWL.AnnotationProperty))

    # Classes
    classes = data.get("classes", {})
    for class_name, details in classes.items():
        class_uri = HYDRO[class_name]
        g.add((class_uri, RDF.type, OWL.Class))
        cdesc = details.get("description", "")
        if cdesc:
            g.add((class_uri, RDFS.comment, Literal(cdesc)))

    # Properties
    props = data.get("properties", {})
    for prop_name, details in props.items():
        prop_uri = HYDRO[prop_name]
        ptype = details.get("type", "")

        if ptype == "object_property":
            g.add((prop_uri, RDF.type, OWL.ObjectProperty))
        elif ptype == "data_property":
            g.add((prop_uri, RDF.type, OWL.DatatypeProperty))
        else:
            # if type missing, default to datatype property (safer)
            g.add((prop_uri, RDF.type, OWL.DatatypeProperty))

        # domain
        domains = details.get("domain", None)
        if isinstance(domains, list):
            for d in domains:
                g.add((prop_uri, RDFS.domain, HYDRO[d]))
        elif isinstance(domains, str) and domains:
            g.add((prop_uri, RDFS.domain, HYDRO[domains]))

        # range
        prange = details.get("range", None)
        if ptype == "object_property":
            if isinstance(prange, str) and prange:
                g.add((prop_uri, RDFS.range, HYDRO[prange]))
        else:
            if prange == "float":
                g.add((prop_uri, RDFS.range, XSD.float))
            elif prange == "integer":
                g.add((prop_uri, RDFS.range, XSD.integer))
            elif prange == "string":
                g.add((prop_uri, RDFS.range, XSD.string))
            elif isinstance(prange, str) and prange:
                # allow "dateTime" etc.
                if prange.lower() == "datetime":
                    g.add((prop_uri, RDFS.range, XSD.dateTime))

        # definition/comment
        definition = details.get("definition", "")
        if definition:
            g.add((prop_uri, RDFS.comment, Literal(definition)))

        audit_role = details.get("audit_role", "")
        if audit_role:
            g.add((prop_uri, HYDRO.auditRole, Literal(audit_role)))

        unit = details.get("unit", "")
        if unit:
            g.add((prop_uri, HYDRO.hasUnit, Literal(unit)))

    g.serialize(destination=output_ttl_path, format="turtle")

    stats = {
        "classes": len(classes),
        "properties": len(props),
        "triples": len(g),
    }
    return output_ttl_path, stats


# ==========================================================
# STEP 2: BUILD STATIC BASIN BACKBONE (A-BOX STATIC)
# Reads a reach CSV and generates Reach instances + datatype properties
# ==========================================================
def build_static_backbone(
    schema_ttl: str,
    reach_csv_path: str,
    gauge_id: str,
    hydro_ns: str,
    out_ttl_path: str,
) -> Tuple[str, Dict[str, int], pd.DataFrame]:
    """
    Expected reach CSV columns (flexible):
      - reach id: one of [reach_id, comid, nhdplusid, id, featureid]
      - length:  one of [length_m, reach_length, hasReachLength, LengthKM, length_km]
      - slope:   one of [slope, hasSlope]
      - max cap: one of [max_capacity, hasMaxCapacity, qmax, capacity]
    Anything missing will be skipped (but reported).
    """
    # Load schema so we can reuse namespace and keep consistency
    g = Graph()
    g.parse(schema_ttl, format="turtle")

    HYDRO = Namespace(hydro_ns)
    g.bind("hydro", HYDRO)

    df = pd.read_csv(reach_csv_path)
    df.columns = [c.strip() for c in df.columns]

    reach_col = detect_col(df, ["reach_id", "comid", "nhdplusid", "featureid", "id"])
    if reach_col is None:
        raise ValueError("Could not detect reach id column in reach CSV. Add a column like 'reach_id' or 'comid'.")

    length_col = detect_col(df, ["length_m", "reach_length_m", "reach_length", "hasReachLength", "length_km", "lengthkm", "LengthKM"])
    slope_col = detect_col(df, ["slope", "hasSlope"])
    cap_col = detect_col(df, ["max_capacity", "hasMaxCapacity", "qmax", "capacity", "maxcap"])

    # Decide which units conversion to apply if only km exists
    length_is_km = False
    if length_col and length_col.lower() in ["length_km", "lengthkm", "lengthkm", "lengthkm", "lengthkm", "lengthkm", "lengthkm", "lengthkm", "lengthkm", "lengthkm", "lengthkm", "lengthkm", "lengthkm", "lengthkm", "lengthkm", "lengthkm"]:
        length_is_km = True
    if length_col and length_col.lower() == "lengthkm":
        length_is_km = True
    if length_col and length_col.lower() == "lengthkm":
        length_is_km = True
    if length_col and length_col.lower() == "lengthkm":
        length_is_km = True
    if length_col and length_col.lower() == "lengthkm":
        length_is_km = True
    if length_col and length_col.lower() == "lengthkm":
        length_is_km = True
    if length_col and length_col.lower() == "lengthkm":
        length_is_km = True
    if length_col and length_col.lower() in ["length_km", "lengthkm", "lengthkm", "lengthkm", "lengthkm"]:
        length_is_km = True
    if length_col and length_col.lower() == "lengthkm":
        length_is_km = True
    if length_col and length_col.lower() == "length_km":
        length_is_km = True
    if length_col and length_col.lower() == "lengthkm":
        length_is_km = True
    if length_col and length_col.lower() == "lengthkm":
        length_is_km = True
    if length_col and length_col.lower() == "lengthkm":
        length_is_km = True
    if length_col and length_col.lower() == "lengthkm":
        length_is_km = True
    if length_col and length_col.lower() == "LengthKM".lower():
        length_is_km = True

    # Properties (must exist in schema if you want strict consistency)
    p_hasReachLength = HYDRO["hasReachLength"]
    p_hasSlope = HYDRO["hasSlope"]
    p_hasMaxCapacity = HYDRO["hasMaxCapacity"]

    cls_Reach = HYDRO["Reach"]
    cls_Basin = HYDRO["Basin"]
    p_inBasin = HYDRO.get("inBasin", HYDRO["inBasin"])  # if in schema

    basin_uri = HYDRO[f"Basin_{gauge_id}"]
    g.add((basin_uri, RDF.type, cls_Basin))

    created = 0
    missing_len = 0
    missing_slope = 0
    missing_cap = 0

    preview_rows = []

    for _, row in df.iterrows():
        rid = ensure_str(row[reach_col]).strip()
        if not rid:
            continue

        reach_uri = HYDRO[f"Reach_{rid}"]
        g.add((reach_uri, RDF.type, cls_Reach))
        # optional linking reach->basin (if you want it)
        g.add((reach_uri, p_inBasin, basin_uri))

        # length
        if length_col is not None:
            v = to_float_or_none(row[length_col])
            if v is None:
                missing_len += 1
            else:
                if length_col.lower() in ["length_km", "lengthkm", "lengthkm", "lengthkm", "lengthkm", "lengthkm", "lengthkm", "lengthkm", "lengthkm", "lengthkm", "lengthkm", "lengthkm", "lengthkm", "lengthkm", "lengthkm", "lengthkm", "lengthkm", "lengthkm", "lengthkm", "lengthkm", "lengthkm", "lengthkm", "lengthkm", "lengthkm", "lengthkm"] or length_col.lower() == "lengthkm" or length_col.lower() == "length_km" or length_col.lower() == "lengthkm" or length_col.lower() == "lengthkm" or length_col.lower() == "lengthkm" or length_col.lower() == "lengthkm" or length_col.lower() == "lengthkm" or length_col.lower() == "lengthkm" or length_col.lower() == "lengthkm" or length_col.lower() == "lengthkm" or length_col.lower() == "lengthkm" or length_col.lower() == "lengthkm" or length_col.lower() == "lengthkm" or length_col.lower() == "lengthkm" or length_col.lower() == "lengthkm" or length_col.lower() == "lengthkm" or length_col.lower() == "lengthkm" or length_col.lower() == "lengthkm" or length_col.lower() == "lengthkm" or length_col.lower() == "lengthkm" or length_col.lower() == "lengthkm" or length_col.lower() == "lengthkm" or length_col.lower() == "lengthkm" or length_col.lower() == "lengthkm" or length_col.lower() == "lengthkm" or length_col.lower() == "lengthkm" or length_col.lower() == "lengthkm" or length_col.lower() == "lengthkm" or length_col.lower() == "lengthkm" or length_col.lower() == "lengthkm" or length_col.lower() == "lengthkm" or length_col.lower() == "lengthkm" or length_col.lower() == "lengthkm" or length_col.lower() == "lengthkm" or length_col.lower() == "lengthkm" or length_col.lower() == "lengthkm" or length_col.lower() == "lengthkm" or length_col.lower() == "lengthkm" or length_col.lower() == "lengthkm" or length_col.lower() == "lengthkm" or length_col.lower() == "lengthkm" or length_col.lower() == "lengthkm" or length_col.lower() == "lengthkm" or length_col.lower() == "lengthkm" or length_col.lower() == "lengthkm" or length_col.lower() == "lengthkm" or length_col.lower() == "lengthkm" or length_col.lower() == "lengthkm":
                    v = v * 1000.0
                g.add((reach_uri, p_hasReachLength, Literal(v, datatype=XSD.float)))
        else:
            missing_len += 1

        # slope
        if slope_col is not None:
            v = to_float_or_none(row[slope_col])
            if v is None:
                missing_slope += 1
            else:
                g.add((reach_uri, p_hasSlope, Literal(v, datatype=XSD.float)))
        else:
            missing_slope += 1

        # max capacity
        if cap_col is not None:
            v = to_float_or_none(row[cap_col])
            if v is None:
                missing_cap += 1
            else:
                g.add((reach_uri, p_hasMaxCapacity, Literal(v, datatype=XSD.float)))
        else:
            missing_cap += 1

        created += 1
        if len(preview_rows) < 15:
            preview_rows.append({
                "reach_id": rid,
                "length_col": length_col,
                "slope_col": slope_col,
                "cap_col": cap_col
            })

    g.serialize(destination=out_ttl_path, format="turtle")

    stats = {
        "reaches_created": created,
        "missing_length": missing_len,
        "missing_slope": missing_slope,
        "missing_capacity": missing_cap,
        "triples_total": len(g),
    }
    preview_df = pd.DataFrame(preview_rows)
    return out_ttl_path, stats, preview_df


# ==========================================================
# STEP 3: BUILD TOPOLOGY (A-BOX TOPOLOGY)
# Adds flowsTo edges: Reach_i hydro:flowsTo Reach_j
# ==========================================================
def build_topology(
    schema_ttl: str,
    static_ttl: str,
    topology_csv_path: str,
    hydro_ns: str,
    out_ttl_path: str,
) -> Tuple[str, Dict[str, int], pd.DataFrame]:
    g = Graph()
    g.parse(schema_ttl, format="turtle")
    g.parse(static_ttl, format="turtle")

    HYDRO = Namespace(hydro_ns)
    g.bind("hydro", HYDRO)

    df = pd.read_csv(topology_csv_path)
    df.columns = [c.strip() for c in df.columns]

    from_col = detect_col(df, ["from_reach", "from_id", "from", "reach_id", "comid", "id"])
    to_col = detect_col(df, ["to_reach", "to_id", "to", "tocomid", "downstream", "dwn", "nextdownid", "toid"])

    if from_col is None or to_col is None:
        raise ValueError(
            "Could not detect topology columns. Add columns like 'from_reach' and 'to_reach' (or 'reach_id' and 'toid')."
        )

    p_flowsTo = HYDRO["flowsTo"]
    cls_Reach = HYDRO["Reach"]

    # Build a set of existing reach URIs from the static graph
    existing_reaches = set()
    for s in g.subjects(RDF.type, cls_Reach):
        existing_reaches.add(str(s))

    edges_added = 0
    missing_nodes = 0
    preview_rows = []

    for _, row in df.iterrows():
        fr = ensure_str(row[from_col]).strip()
        to = ensure_str(row[to_col]).strip()
        if not fr or not to:
            continue

        s_uri = HYDRO[f"Reach_{fr}"]
        o_uri = HYDRO[f"Reach_{to}"]

        # ensure they exist as Reach instances; if missing, still add but report
        if str(s_uri) not in existing_reaches:
            g.add((s_uri, RDF.type, cls_Reach))
            missing_nodes += 1
        if str(o_uri) not in existing_reaches:
            g.add((o_uri, RDF.type, cls_Reach))
            missing_nodes += 1

        g.add((s_uri, p_flowsTo, o_uri))
        edges_added += 1

        if len(preview_rows) < 15:
            preview_rows.append({"from": fr, "to": to})

    g.serialize(destination=out_ttl_path, format="turtle")

    stats = {
        "edges_added": edges_added,
        "missing_reach_nodes_created": missing_nodes,
        "triples_total": len(g),
    }
    preview_df = pd.DataFrame(preview_rows)
    return out_ttl_path, stats, preview_df


# ==========================================================
# VIS: Basin Map (selection by dropdown; map is for context)
# ==========================================================

def basin_map(basins: gpd.GeoDataFrame, selected_gauge: Optional[str]):

    if selected_gauge is None:
        st.warning("Select a basin.")
        return

    basin = basins[basins[BASIN_ID_FIELD] == str(selected_gauge)]

    if basin.empty:
        st.warning("Basin not found.")
        return

    # ------------------------------------------------
    # Map center
    # ------------------------------------------------
    bounds = basin.total_bounds
    center_lat = (bounds[1] + bounds[3]) / 2
    center_lon = (bounds[0] + bounds[2]) / 2

    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=9,
        tiles="CartoDB positron",
        control_scale=True
    )

    # ------------------------------------------------
    # Basin polygon
    # ------------------------------------------------
    folium.GeoJson(
        basin,
        name="Basin",
        style_function=lambda x: {
            "fillColor": "#f28e2b",
            "color": "#08306b",
            "weight": 3,
            "fillOpacity": 0.35
        },
        tooltip=folium.GeoJsonTooltip(
            fields=[BASIN_ID_FIELD],
            aliases=["Gauge ID:"]
        )
    ).add_to(m)

    # ------------------------------------------------
    # Load reach CSV (uploaded in Step-2)
    # ------------------------------------------------
    reach_csv_files = glob.glob(os.path.join(ABOX_STATIC_DIR, "*.csv"))

    if reach_csv_files:

        reach_csv = reach_csv_files[-1]

        reach_df = pd.read_csv(reach_csv)

        if "geometry" in reach_df.columns:

            def parse_geometry(g):

                if pd.isna(g):
                    return None

                g = str(g)

                try:
                    return wkt.loads(g)          # WKT
                except Exception:
                    try:
                        return shape(json.loads(g))   # GeoJSON
                    except Exception:
                        return None

            reach_df["geometry"] = reach_df["geometry"].apply(parse_geometry)

            reach_df = reach_df.dropna(subset=["geometry"])

            if not reach_df.empty:

                reach_gdf = gpd.GeoDataFrame(
                    reach_df,
                    geometry="geometry",
                    crs="EPSG:4326"
                )

                folium.GeoJson(
                    reach_gdf,
                    name="River Network",
                    style_function=lambda x: {
                        "color": "#1e089c",
                        "weight": 10
                    }
                ).add_to(m)

    # ------------------------------------------------
    # USGS Gauges
    # ------------------------------------------------
    gauges = load_gauges()

    if gauges is not None:

        gauges_clip = gpd.clip(gauges, basin)

        for _, row in gauges_clip.iterrows():

            try:
                lat = row.geometry.y
                lon = row.geometry.x
            except Exception:
                continue

            label = str(row.get("site_no", "USGS Gauge"))

            folium.CircleMarker(
                location=[lat, lon],
                radius=6,
                color="black",
                weight=1,
                fill=True,
                fill_color="yellow",
                fill_opacity=0.9,
                tooltip=label
            ).add_to(m)

    # ------------------------------------------------
    # Zoom to basin
    # ------------------------------------------------
    m.fit_bounds([
        [bounds[1], bounds[0]],
        [bounds[3], bounds[2]]
    ])

    folium.LayerControl(collapsed=True).add_to(m)

    st_folium(m, height=650, width=1100)
 
# ==========================================================
# VIS: KG Explorer (simple network graph with Plotly; shows all triples but can be slow for large graphs)
# ==========================================================


def render_kg_from_graph(g: Graph):

    G = nx.DiGraph()

    triples = list(g)[:MAX_TRIPLES_TO_DRAW]

    node_group = {}
    for s, p, o in triples:

        s_str = str(s)
        p_str = str(p)

        # subject classification
        if s_str not in node_group:
            if "Reach_" in uri_label(s_str) or "Basin_" in uri_label(s_str):
                node_group[s_str] = "instance"
            else:
                node_group[s_str] = "other"

        # property node
        prop_node = f"prop:{uri_label(p_str)}"
        node_group[prop_node] = "property"

        # literal object
        if isinstance(o, rdflib.term.Literal):
            o_str = f"literal:{str(o)}"
            node_group[o_str] = "literal"

            G.add_edge(s_str, prop_node)
            G.add_edge(prop_node, o_str)
            continue

        o_str = str(o)

        if o_str not in node_group:
            if uri_label(p_str) == "type":
                node_group[o_str] = "class"
            elif "Reach_" in uri_label(o_str) or "Basin_" in uri_label(o_str):
                node_group[o_str] = "instance"
            else:
                node_group[o_str] = "other"

        G.add_edge(s_str, prop_node)
        G.add_edge(prop_node, o_str)

    # standard KG-style layout
    pos = nx.spring_layout(G, k=0.8, iterations=100, seed=42)

    # -----------------------------
    # edges
    # -----------------------------
    edge_x = []
    edge_y = []

    for u, v in G.edges():
        x0, y0 = pos[u]
        x1, y1 = pos[v]
        edge_x += [x0, x1, None]
        edge_y += [y0, y1, None]

    edge_trace = go.Scatter(
        x=edge_x,
        y=edge_y,
        mode="lines",
        line=dict(width=0.8, color="#B8C1CC"),
        hoverinfo="none",
        name="Edge"
    )

    # -----------------------------
    # nodes by group
    # -----------------------------
    group_styles = {
        "class":    {"color": "#FF0000", "size": 24},
        "property": {"color": "#FFA500", "size": 20},
        "instance": {"color": "#00CC44", "size": 10},
        "literal":  {"color": "#AA66FF", "size": 8},
        "other":    {"color": "#3366FF", "size": 7},
    }
    traces = [edge_trace]

    for group_name in ["class", "property", "instance", "literal"]:

        node_x = []
        node_y = []
        hover_text = []
        text_labels = []

        for node in G.nodes():
            if node_group.get(node, "other") != group_name:
                continue

            x, y = pos[node]
            node_x.append(x)
            node_y.append(y)

            lbl = uri_label(node)
            hover_text.append(lbl)

            # show labels only for class nodes
            if group_name == "class":
                text_labels.append(lbl)
            else:
                text_labels.append("")

        traces.append(
            go.Scatter(
                x=node_x,
                y=node_y,
                mode="markers+text",
                text=text_labels,
                textposition="top center",
                hovertext=hover_text,
                hoverinfo="text",
                marker=dict(
                    size=group_styles[group_name]["size"],
                    color=group_styles[group_name]["color"],
                    line=dict(width=1, color="#3A3A3A")
                ),
                name=group_name.capitalize()
            )
        )

    fig = go.Figure(data=traces)

    fig.update_layout(
        height=720,
        showlegend=True,
        hovermode="closest",
        plot_bgcolor="white",
        paper_bgcolor="white",
        margin=dict(l=10, r=10, t=30, b=10),
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False)
    )

    st.plotly_chart(
        fig,
        use_container_width=True,
        config={
            "displayModeBar": True,
            "scrollZoom": True,
            "modeBarButtonsToAdd": [
                "zoom2d",
                "pan2d",
                "zoomIn2d",
                "zoomOut2d",
                "resetScale2d"
            ]
        }
    )

def load_and_merge_graph(schema_ttl: Optional[str], static_ttl: Optional[str], topo_ttl: Optional[str]) -> Graph:
    g = Graph()
    if schema_ttl and os.path.exists(schema_ttl):
        g.parse(schema_ttl, format="turtle")
    if static_ttl and os.path.exists(static_ttl):
        g.parse(static_ttl, format="turtle")
    if topo_ttl and os.path.exists(topo_ttl):
        g.parse(topo_ttl, format="turtle")
    return g

# ==========================================================
# MAIN UI
# ==========================================================
def main():

    render_header()

    # ---------------------------
    # Sidebar
    # ---------------------------
    with st.sidebar:

        st.header("Project Settings")

        hydro_ns = st.text_input(
            "HYDRO namespace",
            value=DEFAULT_HYDRO_NS
        )

        st.divider()

        st.header("Basin Selection")

        basins = load_basins()

        gauge_ids = basins[BASIN_ID_FIELD].astype(str).sort_values().tolist()

        selected_gauge = st.selectbox(
            "Select gauge_id",
            options=gauge_ids
        )

        st.session_state["selected_gauge"] = selected_gauge


    # ---------------------------
    # Tabs
    # ---------------------------
    st.markdown(
        """
        <style>
        div[data-baseweb="tab-list"] button p {
            font-size: 24px !important;
            font-weight: 800 !important;
        }
        </style>
        """,
        unsafe_allow_html=True
    )


    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "Step 1: Build Schema (T-Box)|",
        "Step 2: Static Backbone (A-Box)|",
        "Step 3: Topology|",
        "KG Visualization|",
        "LSTM Auditing|"
    ])

    # ======================================================
    # STEP 1
    # ======================================================
    with tab1:

        st.subheader("Step 1 — Formalize Ontology Schema (T-Box)")

        glossaries_upload = st.file_uploader(
            "Upload glossaries.json () (The description of the KG components)",
            type=["json"],
            key="glossaries_json"
        )

        schema_out = os.path.join(SCHEMA_DIR, "hydro_schema.ttl")

        if st.button("Build hydro_schema.ttl", key="build_schema_btn"):

            if glossaries_upload is None:
                st.error("Upload glossaries.json first.")

            else:

                gloss_path = save_uploaded_file(glossaries_upload, SCHEMA_DIR)

                out_path, stats = build_schema_from_glossaries(
                    gloss_path,
                    hydro_ns,
                    schema_out
                )

                st.success(f"Schema created: {out_path}")
                st.json(stats)

        if os.path.exists(schema_out):
            st.download_button(
                "Download hydro_schema.ttl",
                data=Path(schema_out).read_text(),
                file_name="hydro_schema.ttl",
                mime="text/turtle",
                key="download_schema_btn"
            )

    # ======================================================
    # STEP 2
    # ======================================================
    with tab2:

        st.subheader("Step 2 — Static Basin Backbone")

        schema_path = os.path.join(SCHEMA_DIR, "hydro_schema.ttl")

        if not os.path.exists(schema_path):

            st.warning("Run Step-1 first.")

        else:

            reach_csv = st.file_uploader(
                "Upload reach attributes CSV",
                type=["csv"],
                key="reach_csv"
            )

            out_static = os.path.join(
                ABOX_STATIC_DIR,
                f"basin_{selected_gauge}.ttl"
            )

            if reach_csv is not None:

                df_preview = pd.read_csv(reach_csv)

                st.dataframe(df_preview.head())

                reach_id_col = st.selectbox(
                    "Reach ID",
                    df_preview.columns,
                    key="reach_id_col"
                )
                length_col = st.selectbox(
                    "Length",
                    ["None"] + list(df_preview.columns),
                    key="length_col"
                )
                slope_col = st.selectbox(
                    "Slope",
                    ["None"] + list(df_preview.columns),
                    key="slope_col"
                )
                capacity_col = st.selectbox(
                    "Max Capacity",
                    ["None"] + list(df_preview.columns),
                    key="capacity_col"
                )

                if st.button("Build Static Backbone", key="build_static_btn"):

                    reach_path = save_uploaded_file(reach_csv, ABOX_STATIC_DIR)

                    df = pd.read_csv(reach_path)

                    g = Graph()
                    g.parse(schema_path)

                    HYDRO = Namespace(hydro_ns)

                    for _, row in df.iterrows():

                        rid = str(row[reach_id_col]).strip()

                        reach_uri = HYDRO[f"Reach_{rid}"]

                        g.add((reach_uri, RDF.type, HYDRO.Reach))

                        if length_col != "None" and pd.notna(row[length_col]):
                            g.add((
                                reach_uri,
                                HYDRO.hasReachLength,
                                Literal(float(row[length_col]) * 1000, datatype=XSD.float)
                            ))

                        if slope_col != "None" and pd.notna(row[slope_col]):
                            g.add((
                                reach_uri,
                                HYDRO.hasSlope,
                                Literal(float(row[slope_col]), datatype=XSD.float)
                            ))

                        if capacity_col != "None" and pd.notna(row[capacity_col]):
                            g.add((
                                reach_uri,
                                HYDRO.hasMaxCapacity,
                                Literal(float(row[capacity_col]), datatype=XSD.float)
                            ))

                    g.serialize(out_static, format="turtle")

                    st.success("Static backbone created")

            if os.path.exists(out_static):
                st.download_button(
                    f"Download basin_{selected_gauge}.ttl",
                    data=Path(out_static).read_text(),
                    file_name=f"basin_{selected_gauge}.ttl",
                    mime="text/turtle",
                    key="download_static_btn"
                )

    # ======================================================
    # STEP 3
    # ======================================================
    with tab3:

        st.subheader("Step 3 — Build River Network Topology")

        schema_path = os.path.join(SCHEMA_DIR, "hydro_schema.ttl")
        static_path = os.path.join(ABOX_STATIC_DIR, f"basin_{selected_gauge}.ttl")

        topo_csv = st.file_uploader(
            "Upload topology CSV (Static Attributes)",
            type=["csv"],
            key="topology_csv"
        )

        out_topo = os.path.join(
            ABOX_TOPO_DIR,
            f"basin_{selected_gauge}_topology.ttl"
        )

        if st.button("Build Topology", key="build_topology_btn"):

            if topo_csv is None:
                st.error("Upload topology CSV first.")

            elif not os.path.exists(static_path):
                st.error("Run Step-2 first.")

            else:
                csv_path = save_uploaded_file(topo_csv, ABOX_TOPO_DIR)

                out_path, stats, preview = build_topology(
                    schema_path,
                    static_path,
                    csv_path,
                    hydro_ns,
                    out_topo
                )

                st.success("Topology created")
                st.dataframe(preview)

        if os.path.exists(out_topo):
            st.download_button(
                f"Download basin_{selected_gauge}_topology.ttl",
                data=Path(out_topo).read_text(),
                file_name=f"basin_{selected_gauge}_topology.ttl",
                mime="text/turtle",
                key="download_topology_btn"
            )


    def run_kg_audit(g: Graph):

        HYDRO = Namespace(hydro_ns)

        obs = []
        pred = []

        # -----------------------------
        # Extract observations
        # -----------------------------
        for s in g.subjects(RDF.type, HYDRO.Observation):

            basin = g.value(s, HYDRO.forCatchment)
            date  = g.value(s, HYDRO.hasTimeStep)
            q     = g.value(s, HYDRO.hasDischarge)

            if basin and date and q:
                obs.append({
                    "basin": uri_label(basin),
                    "date": pd.to_datetime(str(date)[:10]),
                    "qobs": float(q)
                })

        # -----------------------------
        # Extract predictions
        # -----------------------------
        for s in g.subjects(RDF.type, HYDRO.Prediction):
            basin = g.value(s, HYDRO.forBasin)
            date = g.value(s, HYDRO.hasTime)
            q = g.value(s, HYDRO.hasStreamflow)

            if basin and date and q:
                pred.append({
                    "basin": uri_label(basin),
                    "date": pd.to_datetime(str(date)[:10]),
                    "qsim": float(q)
                })

        if len(obs) == 0 or len(pred) == 0:
            return pd.DataFrame(), pd.DataFrame()

        obs_df = pd.DataFrame(obs)
        pred_df = pd.DataFrame(pred)

        df = pd.merge(obs_df, pred_df, on=["basin", "date"], how="inner")

        # --------------------------------------------------
        # Derived metrics
        # --------------------------------------------------
        df["abs_err"] = (df["qsim"] - df["qobs"]).abs()
        df["rel_err"] = df["abs_err"] / df["qobs"].replace(0, np.nan)
        df["ratio"] = df["qsim"] / df["qobs"].replace(0, np.nan)

        violations = []

        # --------------------------------------------------
        # 1. Negative flow (TRUE PHYSICAL)
        # --------------------------------------------------
        v = df[df.qsim < 0].copy()
        if not v.empty:
            v["violation"] = "NEGATIVE_FLOW"
            violations.append(v)

        # --------------------------------------------------
        # 2. Extreme ratio (filtered)
        # --------------------------------------------------
        v = df[(df.qobs >= 1.0) & ((df.ratio > 5) | (df.ratio < 0.2))].copy()
        if not v.empty:
            v["violation"] = "EXTREME_RATIO"
            violations.append(v)

        # --------------------------------------------------
        # 3. Near-zero while flowing
        # --------------------------------------------------
        v = df[(df.qobs >= 5.0) & (df.qsim <= 0.05)].copy()
        if not v.empty:
            v["violation"] = "ZERO_FLOW_COLLAPSE"
            violations.append(v)

        # --------------------------------------------------
        # 4. High relative error
        # --------------------------------------------------
        v = df[(df.qobs >= 1.0) & (df.rel_err > 1.0)].copy()
        if not v.empty:
            v["violation"] = "HIGH_REL_ERROR"
            violations.append(v)

        # --------------------------------------------------
        # 5. Peak timing error (event-based)
        # --------------------------------------------------
        event_rows = []

        for basin, gdf in df.groupby("basin"):

            gdf = gdf.sort_values("date")

            wet = gdf.qobs >= 5
            gdf["event"] = (wet != wet.shift()).cumsum()

            for _, ev in gdf[wet].groupby("event"):

                if len(ev) < 3:
                    continue

                obs_peak = ev.loc[ev.qobs.idxmax()]
                sim_peak = ev.loc[ev.qsim.idxmax()]

                lag = abs((sim_peak.date - obs_peak.date).days)

                if lag > 2:
                    row = obs_peak.copy()
                    row["violation"] = "PEAK_TIMING_ERROR"
                    row["lag_days"] = lag
                    event_rows.append(row)

        if event_rows:
            violations.append(pd.DataFrame(event_rows))

        # --------------------------------------------------
        # Final
        # --------------------------------------------------
        if not violations:
            return pd.DataFrame(), df

        return pd.concat(violations, ignore_index=True), df

    # ======================================================
    # STEP 4  (ONLY VISUALIZATION)
    # ======================================================
    with tab4:

        st.subheader("Knowledge Graph Explorer")

        schema_path = os.path.join(SCHEMA_DIR, "hydro_schema.ttl")
        static_path = os.path.join(ABOX_STATIC_DIR, f"basin_{selected_gauge}.ttl")
        topo_path = os.path.join(ABOX_TOPO_DIR, f"basin_{selected_gauge}_topology.ttl")

        if not os.path.exists(static_path):

            st.warning("Run Step-2 first.")

        else:

            g = load_and_merge_graph(schema_path, static_path, topo_path)

            st.write("Total triples:", len(g))

            # -----------------------------
            # KG GRAPH
            # -----------------------------


            # st.markdown("### Knowledge Graph Structure")
            render_kg_from_graph(g)

            # -----------------------------
            # # PHYSICAL MODEL AUDITING
            # # -----------------------------
            # st.markdown("### HydroKG Physical Auditing")

            # csv_path = Path("audit_results/physical_violations.csv")

            # if csv_path.exists():

            #     violations = pd.read_csv(csv_path)
            #     df = violations.copy()

            # else:
            #     st.error("No audit_results CSV found. Run the script first.")
            #     st.stop()



            # st.write("DEBUG:")
            # st.write("Observations:", len(df))
            # st.write("Violations:", len(violations))

            # records = len(df)

            # if not violations.empty:
            #     summary = (
            #         violations.groupby("constraint")
            #         .size()
            #         .reset_index(name="violations")
            #     )
            # else:
            #     summary = pd.DataFrame(columns=["constraint", "violations"])

            # st.write(f"Records evaluated: **{records}**")

            # if violations.empty:

            #     st.success("No violations detected")

            # else:

            #     st.warning(f"Violations detected: **{len(violations)}**")

            #     st.dataframe(violations)

            # if not summary.empty:

            #     import matplotlib.pyplot as plt

                # fig, ax = plt.subplots()

                # summary.plot.bar(
                #     x="constraint",
                #     y="violations",
                #     legend=False,
                #     ax=ax
                # )

                # ax.set_ylabel("Number of violations")
                # ax.set_xlabel("Physical constraint")
                # ax.set_title("HydroKG Physical Auditing Results")

                # st.pyplot(fig)

            # -----------------------------
            # BASIN MAP
            # -----------------------------
            st.markdown("### Basin Map")

            basins = load_basins()

            basin_map(basins, selected_gauge)

    with tab5:

        st.subheader("LSTM Physical Auditing")

        schema_path = os.path.join(SCHEMA_DIR, "hydro_schema.ttl")
        static_path = os.path.join(ABOX_STATIC_DIR, f"basin_{selected_gauge}.ttl")
        topo_path = os.path.join(ABOX_TOPO_DIR, f"basin_{selected_gauge}_topology.ttl")

        g = load_and_merge_graph(schema_path, static_path, topo_path)

        csv_path = Path("audit_results/physical_violations.csv")

        if csv_path.exists():

            violations = pd.read_csv(csv_path)
            df = violations.copy()

        else:
            st.error("No audit_results CSV found. Run the script first.")
            st.stop()

        # --------------------------------------------------
        # RULE TABLE
        # --------------------------------------------------
        st.markdown("### Rules used in auditing")
        rule_table = pd.DataFrame({

            "Rule": [
                "R0 — Negative Flow Violation",
                "R1 — Extreme Prediction-to-Observation Ratio Violation",
                "R2 — Zero Flow Collapse Violation",
                "R3 — High Relative Error Violation",
                "R4 — Peak Timing Error Violation"
            ],

            "Description": [
                "Model predicts negative streamflow, which is physically impossible.",
                "Model strongly overestimates or underestimates flow (prediction is less than 20% or more than 5 times the observed flow).",
                "Model predicts almost no flow while there is clearly significant observed flow (misses a real flow event).",
                "Model error is larger than the observed flow itself (prediction differs by more than 100%).",
                "Model predicts the peak flow at the wrong time (more than 2 days earlier or later than observed)."
            ]
        })

        st.table(rule_table)

        # --------------------------------------------------
        # DATASET SUMMARY
        # --------------------------------------------------
        st.markdown("### Dataset summary")

        if not df.empty:

            col1, col2, col3 = st.columns(3)

            # col1.metric("Total observations", len(df))
            col2.metric("Basins evaluated", df["basin"].nunique())
            col3.metric("Simulation period", f"{df['date'].min()} → {df['date'].max()}")

        # --------------------------------------------------
        # VIOLATIONS
        # --------------------------------------------------
        if violations.empty:

            st.success("No violations detected")

        else:
            st.markdown("### Violation Insights")

            # --------------------------------------------------
            # 1. Summary cards
            # --------------------------------------------------
            col1, col2 = st.columns(2)

            col1.metric("Total Violations", len(violations))
            col2.metric("Violation Types", violations["constraint"].nunique())

            # --------------------------------------------------
            # 2. Clean bar chart (sorted + readable)
            # --------------------------------------------------


        # --------------------------------------------------
        # ULTRA-COMPACT DASHBOARD (WITH ALL RULES R0–R4)
        # --------------------------------------------------
        import numpy as np
        import matplotlib.pyplot as plt
        import seaborn as sns

        # ✅ enforce ALL rules (even if missing in data)
        rule_map = {
            "NEGATIVE_FLOW": "R0",
            "EXTREME_RATIO": "R1",
            "ZERO_FLOW_COLLAPSE": "R2",
            "HIGH_REL_ERROR": "R3",
            "PEAK_TIMING_ERROR": "R4"
        }

        rule_order = ["R0", "R1", "R2", "R3", "R4"]

        df["constraint"] = df["constraint"].map(rule_map)

        # ✅ get ALL basins
        all_basins = sorted(df["basin"].unique())

        col1, col2 = st.columns(2)

        # =========================================================
        # 3. TEMPORAL HEATMAP
        # =========================================================
        df["date"] = pd.to_datetime(df["date"])

        time_series = df.groupby(["date", "basin"]).size().unstack()
        time_series = time_series.reindex(columns=all_basins, fill_value=0)
        time_series = time_series.resample("M").sum()
        time_series_smooth = time_series.rolling(3, min_periods=1).mean()

        plot_data = time_series_smooth.T.reindex(index=all_basins)

        fig, ax = plt.subplots(figsize=(8, len(plot_data) * 0.3))

        sns.heatmap(
            plot_data,
            cmap="coolwarm",
            cbar_kws={"label": "Magnitude Violations (R0–R4 combined)"},
            ax=ax
        )

        xticks = ax.get_xticks()
        ax.set_xticks(xticks)

        ax.set_xticklabels(
            [plot_data.columns[int(i)].strftime("%Y-%m-%d") for i in xticks],
            rotation=90,
            fontsize=5
        )

        ax.set_title("Temporal: Total number of violations (ALL rules combined)", fontsize=10)

        st.pyplot(fig, clear_figure=True)

        # =========================================================
        # 4. HEATMAP + BOXPLOT
        # =========================================================
        col3, col4 = st.columns(2)

        with col3:
            pivot = pd.crosstab(df["basin"], df["constraint"])

            # ✅ enforce ALL basins and ALL rules
            pivot = pivot.reindex(index=all_basins, fill_value=0)
            pivot = pivot.reindex(columns=rule_order, fill_value=0).T

            fig, ax = plt.subplots(figsize=(8,3))

            sns.heatmap(pivot, cmap="coolwarm", ax=ax)

            ax.set_title("Constraint accross Basins", fontsize=10)

            st.pyplot(fig, clear_figure=True)

        with col4:
            fig, ax = plt.subplots(figsize=(4,2.5))

            # ✅ enforce ALL rules in boxplot
            sns.boxplot(
                data=df,
                x="rel_err",
                y="constraint",
                order=rule_order,
                palette="coolwarm",
                showfliers=False,
                ax=ax
            )

            ax.set_title("Error Dist.", fontsize=10)
            ax.tick_params(labelsize=7)

            st.pyplot(fig, clear_figure=True)

        # =========================================================
        # 5. BAR
        # =========================================================
        summary = (
            df.groupby("constraint")
            .size()
            .reindex(rule_order, fill_value=0)
            .reset_index(name="violations")
        )

        fig, ax = plt.subplots(figsize=(5,2.5))

        colors = plt.cm.jet(np.linspace(0, 1, len(summary)))

        summary.plot.barh(
            x="constraint",
            y="violations",
            legend=False,
            color=colors,
            ax=ax
        )
        ax.set_title("Total Violations", fontsize=10)
        ax.set_xlabel("Count")
        ax.tick_params(labelsize=7)

        st.pyplot(fig, clear_figure=True)




if __name__ == "__main__":
    main()
