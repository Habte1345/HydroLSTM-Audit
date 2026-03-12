
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
BASE_DIR = "/bighome/hdagne1/NFDI4Earth-HydroTurtle"

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
    "F:/Github_repos/HydroLSTM-Audit/data\\camels\\camels_basin_shapes.shp"
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


# ==========================================================
# UI HEADER
# ==========================================================
def render_header():
    col1, col2, col3 = st.columns([1.2, 6, 2.5])

    with col1:
        if os.path.exists(LOGO1):
            st.image(LOGO1, width=120)

    with col2:
        st.markdown(
            """
            <h1 style="text-align:center;color:#2c7fb8;font-size:44px;margin-bottom:0px">
            Hydro Schema Dashboard
            </h1>
            <p style="text-align:center;font-size:22px;margin-top:6px">
            Build T-Box (Schema) and A-Box (Static Backbone + Topology), then Explore the Knowledge Graph
            </p>
            """,
            unsafe_allow_html=True
        )

    with col3:
        if os.path.exists(LOGO2):
            st.markdown("<div style='text-align:right;'>", unsafe_allow_html=True)
            st.image(LOGO2, width=240)
            st.markdown("</div>", unsafe_allow_html=True)

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
        "class":    {"color": "#FF0000", "size": 18},
        "property": {"color": "#FFA500", "size": 12},
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
    tab1, tab2, tab3, tab4 = st.tabs([
        "Step 1: Build Schema (T-Box)",
        "Step 2: Static Backbone (A-Box)",
        "Step 3: Topology (A-Box)",
        "Visualize / Explore"
    ])

    # ======================================================
    # STEP 1
    # ======================================================
    with tab1:

        st.subheader("Step 1 — Formalize Ontology Schema (T-Box)")

        glossaries_upload = st.file_uploader(
            "Upload glossaries.json",
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
            "Upload topology CSV",
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

            render_kg_from_graph(g)

            st.markdown("### Basin Map")

            basins = load_basins()

            basin_map(basins, selected_gauge)

if __name__ == "__main__":
    main()

