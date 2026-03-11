
# ==========================================================
# Hydro Schema Dashboard (T-Box + A-Box Builder + Visualizer)
# Streamlit app that follows your advisor's 3 steps:
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
BASE_DIR = "F:\Github_repos\NFDI4Earth-HydroTurtle"

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
    "data_02371500/shapefiles/camels/camels_basin_shapes.shp"
)

NHD_FLOWLINES_FILE = os.path.join(BASE_DIR, "hydro_data/NHDPlus_flowlines.shp")
USGS_GAUGES_FILE = os.path.join(BASE_DIR, "hydro_data/usgs_gauges.shp")

BASIN_ID_FIELD = "gauge_id"

LOGO1 = os.path.join(ASSETS_DIR, "CIROHLogo_200x200.png")
LOGO2 = os.path.join(BASE_DIR, "NFDI4Earth_logo.png")

# Namespace (use your advisor’s-style consistent namespace – update if needed)
DEFAULT_HYDRO_NS = "https://alabama-hydro.org/ontology/"

# KG visualization safety
MAX_TRIPLES_TO_DRAW =100
MAX_NODES_TO_DRAW = 500


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
# (matches your advisor’s procedure, but uses your namespace)
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
    m = folium.Map(
        location=[39, -98],
        zoom_start=4,
        tiles="CartoDB positron",
        control_scale=True
    )

    def style(feature):
        gid = str(feature["properties"][BASIN_ID_FIELD])
        if selected_gauge and gid == str(selected_gauge):
            return {"fillColor": "#f28e2b", "color": "#08306b", "weight": 3, "fillOpacity": 0.60}
        return {"fillColor": "#f28e2b", "color": "#1f77b4", "weight": 1.2, "fillOpacity": 0.25}

    folium.GeoJson(
        basins,
        style_function=style,
        tooltip=folium.GeoJsonTooltip(fields=[BASIN_ID_FIELD], aliases=["Gauge ID:"]),
        name="Basins",
    ).add_to(m)

    # zoom to selected
    if selected_gauge:
        basin = basins[basins[BASIN_ID_FIELD] == str(selected_gauge)]
        if not basin.empty:
            bounds = basin.total_bounds
            m.fit_bounds([[bounds[1], bounds[0]], [bounds[3], bounds[2]]])

            # rivers
            flowlines = load_flowlines()
            if flowlines is not None:
                rivers = gpd.clip(flowlines, basin)
                if not rivers.empty:
                    folium.GeoJson(
                        rivers,
                        name="Flowlines",
                        style_function=lambda x: {"color": "#08519c", "weight": 2}
                    ).add_to(m)

            # gauges
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

    folium.LayerControl(collapsed=True).add_to(m)
    st_folium(m, height=650, width=1100)


# ==========================================================
# VIS: Knowledge Graph (PyVis) – safe JSON options + avoid literal explosion
# ==========================================================


def render_kg_from_graph(g: Graph):

    net = Network(
        height="650px",
        width="100%",
        directed=True,
        bgcolor="#ffffff",
        font_color="black"
    )

    net.set_options("""
    {
      "interaction": {
        "hover": true,
        "zoomView": true,
        "dragView": true
      },
      "physics": {
        "stabilization": false
      }
    }
    """)

    triples = list(g)
    total = min(len(triples), MAX_TRIPLES_TO_DRAW)

    progress = st.progress(0)

    nodes_added = set()

    for i, (s, p, o) in enumerate(triples[:total]):

        s = str(s)
        p = str(p)

        if s not in nodes_added:
            net.add_node(s, label=uri_label(s), color="#4C78A8")
            nodes_added.add(s)

        if isinstance(o, rdflib.term.Literal):

            o_id = f"lit_{hash(o)}"

            if o_id not in nodes_added:
                net.add_node(o_id, label=str(o)[:25], color="#BBBBBB")
                nodes_added.add(o_id)

            net.add_edge(s, o_id, label=uri_label(p))

        else:

            o = str(o)

            if o not in nodes_added:
                net.add_node(o, label=uri_label(o), color="#F58518")
                nodes_added.add(o)

            net.add_edge(s, o, label=uri_label(p))

        progress.progress((i + 1) / total)

    html_path = os.path.join(VIZ_DIR, "kg.html")

    net.save_graph(html_path)

    progress.empty()

    with open(html_path, "r") as f:
        components.html(f.read(), height=700)


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

        if st.button("Build hydro_schema.ttl"):

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

                reach_id_col = st.selectbox("Reach ID", df_preview.columns)
                length_col = st.selectbox("Length", ["None"] + list(df_preview.columns))
                slope_col = st.selectbox("Slope", ["None"] + list(df_preview.columns))
                capacity_col = st.selectbox("Max Capacity", ["None"] + list(df_preview.columns))

                if st.button("Build Static Backbone"):

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
                            g.add((reach_uri,
                                   HYDRO.hasReachLength,
                                   Literal(float(row[length_col])*1000,
                                           datatype=XSD.float)))

                        if slope_col != "None" and pd.notna(row[slope_col]):
                            g.add((reach_uri,
                                   HYDRO.hasSlope,
                                   Literal(float(row[slope_col]),
                                           datatype=XSD.float)))

                        if capacity_col != "None" and pd.notna(row[capacity_col]):
                            g.add((reach_uri,
                                   HYDRO.hasMaxCapacity,
                                   Literal(float(row[capacity_col]),
                                           datatype=XSD.float)))

                    g.serialize(out_static, format="turtle")

                    st.success("Static backbone created")


    # ======================================================
    # STEP 3
    # ======================================================
    with tab3:

        st.subheader("Step 3 — Build River Network Topology")

        schema_path = os.path.join(SCHEMA_DIR, "hydro_schema.ttl")
        static_path = os.path.join(ABOX_STATIC_DIR, f"basin_{selected_gauge}.ttl")

        topo_csv = st.file_uploader(
            "Upload topology CSV",
            type=["csv"]
        )

        out_topo = os.path.join(
            ABOX_TOPO_DIR,
            f"basin_{selected_gauge}_topology.ttl"
        )

        if st.button("Build Topology"):

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




# # ==========================================================
# # Hydro Schema Dashboard
# # ==========================================================

# import os
# import glob
# import json

# import streamlit as st
# import pandas as pd
# import geopandas as gpd

# import folium
# from streamlit_folium import st_folium

# import rdflib
# from pyvis.network import Network
# import streamlit.components.v1 as components

# import triple_creation_from_list_function
# import Fileter_unique_triples_fucntion
# import triple_creation_from_string_function
# import observation_mapping_function
# import time_variables_creation_from_csv_funtion
# import print_RDF_in_turtle_file_fuction
# from format_floats_DF_readed_values_function import format_floats_to_string


# # ==========================================================
# # STREAMLIT CONFIG
# # ==========================================================

# st.set_page_config(
#     page_title="Hydro Schema: A CONUS-Level RDF Converter Tool for Hydrological Data",
#     page_icon="🌊",
#     layout="wide"
# )

# # ==========================================================
# # PATHS
# # ==========================================================

# BASE_DIR = "/bighome/hdagne1/NFDI4Earth-HydroTurtle"

# RDF_OUTPUT_DIR = os.path.join(BASE_DIR,"RDF")
# os.makedirs(RDF_OUTPUT_DIR,exist_ok=True)

# BASIN_POLYGONS_FILE = os.path.join(
# BASE_DIR,
# "data_02371500/shapefiles/camels/camels_basin_shapes.shp"
# )

# # NHDPLUS FLOWLINES
# NHD_FLOWLINES_FILE = os.path.join(
# BASE_DIR,
# "hydro_data/NHDPlus_flowlines.shp"
# )

# # USGS GAUGES
# USGS_GAUGES_FILE = os.path.join(
# BASE_DIR,
# "hydro_data/usgs_gauges.shp"
# )

# BASIN_ID_FIELD="gauge_id"

# LOGO1=os.path.join(BASE_DIR,"assets/CIROHLogo_200x200.png")
# LOGO2="/bighome/hdagne1/NFDI4Earth-HydroTurtle/NFDI4Earth_logo.png"

# MAX_KG_EDGES=250


# # ==========================================================
# # HEADER
# # ==========================================================
# def render_header():

#     col1, col2, col3 = st.columns([1.2, 6, 2.5])

#     # Left logo
#     with col1:
#         if os.path.exists(LOGO1):
#             st.image(LOGO1, width=120)

#     # Title
#     with col2:

#         st.markdown(
#         """
#         <h1 style="text-align:center;color:#2c7fb8;font-size:48px">
#         Hydro Schema: A CONUS-Level RDF Converter Tool for Hydrological Data
#         </h1>

#         <p style="text-align:center;font-size:28px">
#         CONUS Hydrological RDF Converter & Knowledge Graph Explorer
#         </p>
#         """,
#         unsafe_allow_html=True
#         )

#     # Right logo (large and aligned)
#     with col3:

#         if os.path.exists(LOGO2):

#             st.markdown(
#             "<div style='text-align:right;'>",
#             unsafe_allow_html=True
#             )

#             st.image(LOGO2, width=260)

#             st.markdown(
#             "</div>",
#             unsafe_allow_html=True
#             )

#     st.divider()


# # ==========================================================
# # NEW HELPER FUNCTION (ADDED)
# # ==========================================================

# def extract_gauge_id(filename):
#     """
#     Extract basin gauge id from CAMELS file name
#     camels_02371500_dynamic2.csv -> 02371500
#     """

#     name=os.path.basename(filename)

#     parts=name.split("_")

#     if len(parts)>=2:
#         return parts[1]

#     return None


# # ==========================================================
# # HELPERS
# # ==========================================================

# def load_json(file_path):

#     with open(file_path,"r") as f:
#         return json.load(f)


# def save_file(file):

#     if file is None:
#         return None

#     path=file.name

#     with open(path,"wb") as f:
#         f.write(file.getbuffer())

#     return path


# def uri_label(uri):

#     uri=str(uri)

#     if "#" in uri:
#         return uri.split("#")[-1]

#     if "/" in uri:
#         return uri.split("/")[-1]

#     return uri


# # ==========================================================
# # DATA LOADING
# # ==========================================================

# @st.cache_data
# def load_basins():

#     gdf=gpd.read_file(BASIN_POLYGONS_FILE)

#     gdf[BASIN_ID_FIELD]=gdf[BASIN_ID_FIELD].astype(str)

#     if gdf.crs!="EPSG:4326":
#         gdf=gdf.to_crs(4326)

#     return gdf


# @st.cache_data
# def load_flowlines():

#     if not os.path.exists(NHD_FLOWLINES_FILE):
#         return None

#     gdf=gpd.read_file(NHD_FLOWLINES_FILE)

#     if gdf.crs!="EPSG:4326":
#         gdf=gdf.to_crs(4326)

#     return gdf


# @st.cache_data
# def load_gauges():

#     if not os.path.exists(USGS_GAUGES_FILE):
#         return None

#     gdf=gpd.read_file(USGS_GAUGES_FILE)

#     if gdf.crs!="EPSG:4326":
#         gdf=gdf.to_crs(4326)

#     return gdf


# # ==========================================================
# # BASIN MAP WITH HYDRO LAYERS (CORRECTED)
# # ==========================================================
# def basin_map(basins, selected_gauge):

#     import folium
#     from folium import plugins
#     from streamlit_folium import st_folium

#     # ------------------------------------------------------------
#     # Base map
#     # ------------------------------------------------------------

#     m = folium.Map(
#         location=[39, -98],
#         zoom_start=4,
#         tiles="CartoDB positron",
#         control_scale=True
#     )

#     # Additional basemaps (with attribution)
#     folium.TileLayer(
#         tiles="CartoDB dark_matter",
#         name="Dark",
#         attr="© OpenStreetMap contributors © CARTO",
#         show=False
#     ).add_to(m)

#     folium.TileLayer(
#         tiles="OpenStreetMap",
#         name="OpenStreetMap",
#         attr="© OpenStreetMap contributors",
#         show=False
#     ).add_to(m)

#     folium.TileLayer(
#         tiles="Stamen Terrain",
#         name="Terrain",
#         attr="Map tiles by Stamen Design, CC BY 3.0 — Map data © OpenStreetMap contributors",
#         show=False
#     ).add_to(m)

#     # ------------------------------------------------------------
#     # Map tools
#     # ------------------------------------------------------------

#     plugins.MiniMap(toggle_display=True).add_to(m)
#     plugins.Fullscreen().add_to(m)

#     plugins.MeasureControl(
#         primary_length_unit="kilometers"
#     ).add_to(m)

#     plugins.MousePosition(
#         position="bottomleft",
#         prefix="Lat/Lon:"
#     ).add_to(m)

#     # ------------------------------------------------------------
#     # Basin style
#     # ------------------------------------------------------------

#     def style(feature):

#         gid = str(feature["properties"][BASIN_ID_FIELD])

#         # Selected basin
#         if selected_gauge and gid == str(selected_gauge):

#             return {
#                 "fillColor": "#f28e2b",   # orange
#                 "color": "#08306b",       # dark blue border
#                 "weight": 4,
#                 "fillOpacity": 0.65
#             }

#         # Other basins
#         return {
#             "fillColor": "#3413eb",      # orange fill
#             "color": "#1f77b4",          # blue border
#             "weight": 1.5,
#             "fillOpacity": 0.35
#         }

#     folium.GeoJson(
#         basins,
#         style_function=style,
#         highlight_function=lambda x: {
#             "weight":3,
#             "color":"yellow"
#         },
#         tooltip=folium.GeoJsonTooltip(
#             fields=[BASIN_ID_FIELD],
#             aliases=["Gauge ID:"]
#         )
#     ).add_to(m)

#     # ------------------------------------------------------------
#     # Selected basin zoom
#     # ------------------------------------------------------------

#     basin = None

#     if selected_gauge:

#         basin = basins[basins[BASIN_ID_FIELD] == str(selected_gauge)]

#         if not basin.empty:

#             bounds = basin.total_bounds

#             m.fit_bounds([
#                 [bounds[1], bounds[0]],
#                 [bounds[3], bounds[2]]
#             ])

#     # ------------------------------------------------------------
#     # River networks
#     # ------------------------------------------------------------

#     if basin is not None and not basin.empty:

#         flowlines = load_flowlines()

#         if flowlines is not None:

#             rivers = gpd.clip(flowlines, basin)

#             if not rivers.empty:

#                 # Detect stream order column
#                 order_field = None

#                 for c in ["StreamOrde","STREAMORDE","streamorde","StreamOrder"]:
#                     if c in rivers.columns:
#                         order_field = c
#                         break

#                 if order_field:

#                     main_rivers = rivers[rivers[order_field] >= 5]
#                     tributaries = rivers[rivers[order_field] < 5]

#                 else:

#                     main_rivers = rivers
#                     tributaries = rivers.iloc[0:0]

#                 # Tributaries
#                 folium.GeoJson(
#                     tributaries,
#                     name="Tributaries",
#                     style_function=lambda x:{
#                         "color":"#6aaed6",
#                         "weight":1.5
#                     }
#                 ).add_to(m)

#                 # Main rivers
#                 folium.GeoJson(
#                     main_rivers,
#                     name="Main Rivers",
#                     style_function=lambda x:{
#                         "color":"#08519c",
#                         "weight":3
#                     }
#                 ).add_to(m)

#     # ------------------------------------------------------------
#     # USGS gauges
#     # ------------------------------------------------------------

#     if basin is not None and not basin.empty:

#         gauges = load_gauges()

#         if gauges is not None:

#             gauges_clip = gpd.clip(gauges, basin)

#             if not gauges_clip.empty:

#                 for _, row in gauges_clip.iterrows():

#                     try:

#                         lat = row.geometry.y
#                         lon = row.geometry.x

#                     except:
#                         continue

#                     label = str(row.get("site_no","USGS Gauge"))

#                     folium.CircleMarker(
#                         location=[lat, lon],
#                         radius=6,
#                         color="black",
#                         weight=1,
#                         fill=True,
#                         fill_color="yellow",
#                         fill_opacity=0.9,
#                         tooltip=label
#                     ).add_to(m)

#     # ------------------------------------------------------------
#     # Legend
#     # ------------------------------------------------------------

#     legend_html = """
#     <div style="
#         position: fixed;
#         bottom: 20px;
#         right: 20px;
#         z-index:9999;
#         background:white;
#         padding:10px;
#         border:2px solid grey;
#         font-size:13px;
#     ">
#     <b>Map Legend</b><br>
#     <i style="background:#ff4d4d;width:15px;height:10px;display:inline-block"></i> Selected Basin<br>
#     <i style="background:#2c7fb8;width:15px;height:10px;display:inline-block"></i> Other Basins<br>
#     <span style="color:#08519c">━━</span> Main Rivers<br>
#     <span style="color:#6aaed6">━━</span> Tributaries<br>
#     ● USGS Gauge
#     </div>
#     """

#     m.get_root().html.add_child(folium.Element(legend_html))

#     # ------------------------------------------------------------
#     # Layer control
#     # ------------------------------------------------------------

#     folium.LayerControl(collapsed=False).add_to(m)

#     # ------------------------------------------------------------
#     # Render map
#     # ------------------------------------------------------------

#     state = st_folium(m, height=650, width=1200)

#     # Handle basin click
#     if state:

#         clicked = state.get("last_object_clicked")

#         if clicked:

#             props = clicked.get("properties")

#             if props and BASIN_ID_FIELD in props:

#                 return str(props[BASIN_ID_FIELD])

#     return selected_gauge

# # ==========================================================
# # KNOWLEDGE GRAPH
# # ==========================================================

# def render_kg(ttl_path):

#     graph = rdflib.Graph()
#     graph.parse(ttl_path, format="turtle")

#     net = Network(
#         height="550px",
#         width="100%",
#         directed=True,
#         bgcolor="#ffffff",
#         font_color="black"
#     )

#     net.set_options("""
#     {
#     "interaction": {
#         "zoomView": true,
#         "dragView": true,
#         "dragNodes": true,
#         "hover": true
#     },
#     "physics": {
#         "enabled": true,
#         "stabilization": false
#     }
#     }
#     """)

#     nodes_added = set()
#     triples = list(graph)

#     for i,(s,p,o) in enumerate(triples):

#         if i >= MAX_KG_EDGES:
#             break

#         s = str(s)
#         p = str(p)
#         o = str(o)

#         color = "#777"

#         if "sensor" in s:
#             color = "orange"

#         if "observation" in s:
#             color = "blue"

#         if "result" in s:
#             color = "green"

#         if s not in nodes_added:
#             net.add_node(s, label=uri_label(s), color=color)
#             nodes_added.add(s)

#         if o not in nodes_added:
#             net.add_node(o, label=uri_label(o), color="#999")
#             nodes_added.add(o)

#         net.add_edge(s, o, label=uri_label(p))

#     html = os.path.join(RDF_OUTPUT_DIR, "kg.html")

#     net.save_graph(html)

#     with open(html, "r") as f:
#         components.html(f.read(), height=550)


# # ==========================================================
# # RDF PROCESS
# # ==========================================================

# def process_csv_file(df,mapping,prefixes,file_name):

#     all_triples=[]

#     for index,row in df.iterrows():

#         sensor_ID=format_floats_to_string(row["ID"])

#         sensor=mapping["sensorDesignation"][0]+sensor_ID

#         all_triples.append([
#         sensor,
#         mapping["sensorDesignation"][1],
#         mapping["sensorDesignation"][2]
#         ])

#     unique_triples=Fileter_unique_triples_fucntion.get_unique_triples(all_triples)

#     output=os.path.join(
#     RDF_OUTPUT_DIR,
#     file_name+"_g.ttl"
#     )

#     print_RDF_in_turtle_file_fuction.write_triples_to_turtle(
#     unique_triples,
#     prefixes,
#     output
#     )

#     return output,len(unique_triples)


# # ==========================================================
# # MAIN UI
# # ==========================================================

# def home_page():

#     render_header()

#     left,center,right=st.columns([1.2,2.3,2])


#     # LEFT PANEL

#     with left:

#         st.subheader("Data Import")

#         csv_files=st.file_uploader(
#         "Upload CAMELS CSV",
#         type=["csv"],
#         accept_multiple_files=True
#         )

#         mapping_json=st.file_uploader("Mapping JSON",type=["json"])
#         prefixes_json=st.file_uploader("Prefixes JSON",type=["json"])


#         if st.button("Convert CSV → RDF"):

#             mapping_data=load_json(save_file(mapping_json))
#             prefix_json=load_json(save_file(prefixes_json))

#             prefix_data=prefix_json["prefixes"]

#             for file in csv_files:

#                 df=pd.read_csv(file)

#                 gauge=extract_gauge_id(file.name)

#                 df["ID"]=gauge

#                 ttl,triples=process_csv_file(
#                 df,
#                 mapping_data,
#                 prefix_data,
#                 file.name
#                 )

#                 st.success(f"{gauge} → {triples} triples")

#                 st.session_state["selected_basin"]=gauge


#     # CENTER PANEL (MAP)

#     with center:

#         st.subheader("Basin Explorer")

#         basins=load_basins()

#         selected=st.session_state.get("selected_basin")

#         selected=basin_map(basins,selected)

#         st.session_state["selected_basin"]=selected


#     # RIGHT PANEL (RDF + KG)

#     with right:

#         selected=st.session_state.get("selected_basin")

#         if selected:

#             st.subheader(f"Basin {selected}")

#             pattern=os.path.join(RDF_OUTPUT_DIR,f"*{selected}*.ttl")

#             files=glob.glob(pattern)

#             if files:

#                 ttl=files[0]

#                 text=open(ttl).read()

#                 st.download_button("Download TTL",text)

#                 st.text_area("Preview",text[:2500],height=200)

#                 st.subheader("Knowledge Graph")

#                 render_kg(ttl)

#             else:

#                 st.warning("No RDF file generated yet")


# # ==========================================================
# # MAIN
# # ==========================================================

# if __name__=="__main__":

#     home_page()








# from time_inputs_from_user_management_without_st import time_with_each_column_user_input_without_st
# import streamlit as st
# import json
# import pandas as pd
# from io import StringIO
# import time
# import geopandas as gpd
# from pyproj import Transformer
# from tqdm import tqdm

# # Import custom functions
# import triple_creation_from_list_function
# import Fileter_unique_triples_fucntion
# import triple_creation_from_string_function
# import observation_mapping_function
# import time_variables_creation_from_csv_funtion
# import print_RDF_in_turtle_file_fuction
# from extract_sensor_id_from_file_fuciton import extract_sensor_id
# from categorize_file_function import categorize_file
# from check_values_in_a_list_fucntion import check_values_in_list
# from format_floats_DF_readed_values_function import format_floats_to_string
# from delimeter_selection_funciton import delimiter_selection
# from transform_coordinates_func import transform_coordinates, ensure_wgs84
# from time_inputs_from_user_management import time_with_each_column_user_input

# def load_json(file_path):
#     try:
#         with open(file_path, 'r') as file:
#             return json.load(file)
#     except Exception as e:
#         st.error(f"Error loading JSON file: {e}")
#         return None


# def save_file(file):
#     try:
#         file_path = file.name
#         with open(file_path, 'wb') as f:
#             f.write(file.getbuffer())
#         return file_path
#     except Exception as e:
#         st.error(f"Error saving file: {e}")
#         return None


# def combine_json_files(json_file_paths):
#     combined_data = {}
#     for file_path in json_file_paths:
#         if file_path:
#             try:
#                 with open(file_path, 'r') as f:
#                     data = json.load(f)
#                     combined_data.update(data)
#             except Exception as e:
#                 st.error(f"Error combining JSON file {file_path}: {e}")

#     return combined_data
# import os
# RDF_OUTPUT_DIR = r"C:\Users\hdagne1\Box\NRT_Project_2026_Spring\Habtamu\NFDI4Earth-HydroTurtle\RDF"
# os.makedirs(RDF_OUTPUT_DIR, exist_ok=True)

# def process_csv_file(dataframe, mapping_json_data, prefixes_json_data, time_data_json, is_time_dependant_csv, gauge_or_catchment,
#                      sensor_ID_column_heading, csv_file_name, parsing_method, date_col, time_col, date_format, time_format):
#     all_triples = []

#     if sensor_ID_column_heading not in dataframe.columns:
#         st.error(f"Selected sensor ID column '{sensor_ID_column_heading}' does not exist in the DataFrame.")
#         return None

#     column_headings = dataframe.columns.tolist()
#     confirmed_matches = [key for key in mapping_json_data.keys() if key in column_headings]
    
#     no_of_inputs = 0

#     for index, row in dataframe.iterrows():
#         sensor_ID = format_floats_to_string(row[sensor_ID_column_heading])
#         sensor_designation = mapping_json_data['sensorDesignation'][0] + sensor_ID
#         sensor_definition = [sensor_designation, mapping_json_data['sensorDesignation'][1], mapping_json_data['sensorDesignation'][2]]
#         all_triples.append(sensor_definition)

#         for index_of_iterating_column, heading in enumerate(confirmed_matches):
#             cell_value = row[heading]
#             no_of_inputs += 1
#             matched_vocabulary_from_mapping_json = mapping_json_data[heading]

#             if not is_time_dependant_csv:
#                 element = mapping_json_data['sensorDesignation'][0]
#                 identification_number = sensor_ID

#                 if isinstance(matched_vocabulary_from_mapping_json, list):
#                     triple_creation_from_list_function.process_given_list(
#                         matched_vocabulary_from_mapping_json, element, identification_number, cell_value, all_triples)
#                 elif isinstance(matched_vocabulary_from_mapping_json, str):
#                     triple_creation_from_string_function.process_given_string(
#                         matched_vocabulary_from_mapping_json, element, identification_number, cell_value, all_triples)
#             else:
#                 element = mapping_json_data['observation_designation'][0]
#                 has_result_term = "sosa:hasResult"
#                 has_sim_result_term = "sosa:hasSimpleResult"
#                 result_time_term = "sosa:resultTime"
#                 observation_definition = "n4e_hyd:observation_"

#                 #observation_no = (index * len(confirmed_matches) + (index_of_iterating_column + 1))

#                 #updated observation_no including sensor ID
#                 observation_no = (str(sensor_ID) + '_' + gauge_or_catchment + '_' +
#                                   str((index * len(confirmed_matches) + (index_of_iterating_column + 1))))

#                 # observation_date_time = time_variables_creation_from_csv_funtion.parse_csv_row(
#                 #     dataframe, 
#                 #     time_data_json, 
#                 #     index, 
#                 #     parsing_method, 
#                 #     date_col, 
#                 #     time_col, 
#                 #     date_format, time_format)
                
#                 observation_date_time = time_variables_creation_from_csv_funtion.parse_csv_row(
#                     dataframe=dataframe,
#                     time_data_json_dictionary=None,   # IMPORTANT
#                     row_number=index,
#                     parsing_method="single_date_column",
#                     date_col="date",
#                     date_format="%Y-%m-%d"
#                     )

#                 observation_mapping_function.observation_mapping(
#                     matched_vocabulary_from_mapping_json, observation_date_time, sensor_ID, all_triples,
#                     observation_definition, result_time_term, has_result_term, has_sim_result_term,
#                     observation_no, cell_value)

#     unique_triples = Fileter_unique_triples_fucntion.get_unique_triples(all_triples)
#     # output_turtle_file = csv_file_name.split(".")[0] + ".ttl"

#     output_turtle_file = os.path.join(RDF_OUTPUT_DIR,csv_file_name.split(".")[0] + "_" + gauge_or_catchment + ".ttl")

#     print_RDF_in_turtle_file_fuction.write_triples_to_turtle(unique_triples, prefixes_json_data, output_turtle_file)
#     # st.write(f"No of inputs converted: {no_of_inputs}")
#     return output_turtle_file, len(unique_triples)



# def home_page():

#     st.markdown(
#         "<h1 style='color:#2c7fb8;'>Hydro Schema: A CONUS-Level RDF Converter Tool for Hydrological Data</h1>",
#         unsafe_allow_html=True
#     )

#     col1, col2 = st.columns(2)
    

#     with col2:
#         st.image(
#             r"C:\Users\hdagne1\Box\NRT_Project_2026_Spring\Habtamu\NFDI4Earth-HydroTurtle\NFDI4Earth_logo.png",
#             width=2000
#         )

#     file_type = st.radio("Select file type:", ["CSV", "Shapefile"], index=0)
#     # ==========================================================
#     # ======================== CSV MODE ========================
#     # ==========================================================
#     if file_type == "CSV":

#         csv_files = st.file_uploader(
#             "Upload CSV files (CAMELS – multiple allowed)",
#             type=["csv"],
#             accept_multiple_files=True
#         )

#         mapping_json_files = st.file_uploader(
#             "Upload Mapping JSON files",
#             type=["json"],
#             accept_multiple_files=True
#         )

#         prefixes_json_files = st.file_uploader(
#             "Upload Prefixes JSON files",
#             type=["json"],
#             accept_multiple_files=True
#         )

#         if not (csv_files and mapping_json_files and prefixes_json_files):
#             return

#         # ----------------------------------------------------------
#         # LOAD JSON FILES (ONCE)
#         # ----------------------------------------------------------
#         mapping_json_file_paths = [save_file(f) for f in mapping_json_files]
#         prefixes_json_file_paths = [save_file(f) for f in prefixes_json_files]

#         combined_prefixes_json_data = combine_json_files(prefixes_json_file_paths)

#         time_data_json = load_json(
#             r"C:\Users\hdagne1\Box\NRT_Project_2026_Spring\Habtamu\NFDI4Earth-HydroTurtle\mappings\mapping_time.json"
#         )

#         # ----------------------------------------------------------
#         # GLOBAL SETTINGS (CAMELS – APPLY TO ALL FILES)
#         # ----------------------------------------------------------
#         st.subheader("Global settings (applied to ALL CSV files)")

#         gauge_or_catchment = "g"
#         is_time_dependant_csv = True
#         sensor_ID_column_heading = "ID"
#         delimiter = ","

#         selected_mapping_json_file = st.selectbox(
#             "Select mapping JSON (applies to all files)",
#             mapping_json_file_paths,
#             key="mapping_json_global"
#         )

#         mapping_json_data = load_json(selected_mapping_json_file)

#         st.info(
#             "Assumptions applied:\n"
#             "- Dataset type: Gauge\n"
#             "- Time dependent: YES\n"
#             "- Date column: 'date'\n"
#             "- Sensor ID: extracted from filename"
#         )

#         # ----------------------------------------------------------
#         # TIME PARSING (USE REPO FUNCTION — VALID METHOD)
#         # ----------------------------------------------------------
#         (
#             parsing_method,
#             _,
#             date_col,
#             time_col,
#             date_format,
#             time_format
#         ) = time_with_each_column_user_input_without_st(time_data_json)

#         # ----------------------------------------------------------
#         # PROCESS ALL FILES
#         # ----------------------------------------------------------
#         if st.button("Convert ALL CSV files to Turtle"):

#             progress = st.progress(0)
#             total = len(csv_files)

#             for i, csv_file in enumerate(csv_files):

#                 csv_file_name = csv_file.name
#                 st.write(f"Processing **{csv_file_name}**")

#                 csv_content = csv_file.getvalue().decode("utf-8")
#                 df = pd.read_csv(StringIO(csv_content), delimiter=delimiter)

#                 # --------------------------------------------------
#                 # EXTRACT gauge_id FROM FILENAME
#                 # camels_01031500.csv → 01031500
#                 # --------------------------------------------------
#                 try:
#                     extracted_id = csv_file_name.split("_")[1].split(".")[0]
#                 except Exception:
#                     st.error(f"Cannot extract gauge ID from {csv_file_name}")
#                     continue

#                 df["ID"] = extracted_id

#                 start_time = time.time()

#                 result = process_csv_file(
#                     df,
#                     mapping_json_data,
#                     combined_prefixes_json_data,
#                     time_data_json,
#                     is_time_dependant_csv,
#                     gauge_or_catchment,
#                     sensor_ID_column_heading,
#                     csv_file_name,
#                     parsing_method,
#                     date_col,
#                     time_col,
#                     date_format,
#                     time_format
#                 )

#                 elapsed = time.time() - start_time

#                 if result:
#                     output_turtle_file, num_triples = result
#                     st.success(
#                         f"{csv_file_name} → {num_triples} triples "
#                         f"({elapsed:.1f}s)"
#                     )

#                 progress.progress((i + 1) / total)

#             st.success("All files processed successfully.")

#     # ==========================================================
#     # ===================== SHAPEFILE MODE =====================
#     # ==========================================================
#     else:
#         st.info("Shapefile mode unchanged.")


# if __name__ == "__main__":
#     home_page()








# def home_page():
#     st.title("HydroSecure - RDF Turtle Converter")
#     st.image(r"C:\Users\hdagne1\Box\NRT_Project_2026_Spring\Habtamu\NFDI4Earth-HydroTurtle\NFDI4Earth_logo.png", width=450)

#     file_type = st.radio("Select file type:", ["CSV", "Shapefile"], index=0)

#     if file_type == "CSV":
#         csv_files = st.file_uploader("Upload CSV files", type=["csv"], accept_multiple_files=True)
#         mapping_json_files = st.file_uploader("Upload Mapping JSON files", type=['json'], accept_multiple_files=True)
#         prefixes_json_files = st.file_uploader("Upload Prefixes JSON files", type=["json"], accept_multiple_files=True)


#         if csv_files and mapping_json_files and prefixes_json_files:
#             mapping_json_file_paths = [save_file(f) for f in mapping_json_files]
#             prefixes_json_file_paths = [save_file(f) for f in prefixes_json_files]
#             combined_prefixes_json_data = combine_json_files(prefixes_json_file_paths)

#             # time_data_json = load_json('mapping_time.json')
#             time_data_json = load_json(
#                 r"C:\Users\hdagne1\Box\NRT_Project_2026_Spring\Habtamu\NFDI4Earth-HydroTurtle\mappings\mapping_time.json"
#             )


#             for csv_file_index, csv_file in enumerate(csv_files):
#                 csv_file_name = csv_file.name
#                 st.markdown(f"### Processing CSV file: ***{csv_file_name}***")

#                 # Add dropdown for gauge or catchment
#                 # gauge_or_catchment_selection = st.selectbox(
#                 #     "Select the type of dataset:",
#                 #     options=["gauge", "catchment", "other"],
#                 #     index=0  # Default to "gauge"
#                 # )

#                 gauge_or_catchment_selection = st.selectbox(
#                     "Select the type of dataset:",
#                     options=["gauge", "catchment", "other"],
#                     index=0,
#                     key=f"gauge_or_catchment_{csv_file.name}"
#                     )


#                 # Assign variable based on selection
#                 if gauge_or_catchment_selection == "gauge":
#                     gauge_or_catchment = "g"
#                 elif gauge_or_catchment_selection == "catchment":
#                     gauge_or_catchment = "c"
#                 else:
#                     gauge_or_catchment = "o"

#                 st.write(
#                     f"Selected type: {gauge_or_catchment_selection.capitalize()} (Assigned as '{gauge_or_catchment.upper()}')")


#                 selected_mapping_json_file = st.selectbox(f"Select mapping JSON file for {csv_file_name}", mapping_json_file_paths)
#                 mapping_json_data = load_json(selected_mapping_json_file)
#                 if f"{csv_file.name}_delimiter" not in st.session_state:
#                     st.session_state[f"{csv_file.name}_delimiter"] = ","  # Default delimiter

#                 delimiter = st.selectbox(
#                     f"Select delimiter for {csv_file.name}",
#                     [",", ";", "\t", " "],
#                     index=[",", ";", "\t", " "].index(st.session_state[f"{csv_file.name}_delimiter"])
#                 )

#                 if delimiter != st.session_state[f"{csv_file.name}_delimiter"]:
#                     st.session_state[f"{csv_file.name}_delimiter"] = delimiter

#                 csv_content = csv_file.getvalue().decode("utf-8")
#                 csv_file_data_frame = pd.read_csv(StringIO(csv_content), delimiter=delimiter)
#                 st.session_state[f"{csv_file.name}_data_frame"] = csv_file_data_frame

#                 #st.write(csv_file_data_frame.head())  # Preview the DataFrame

#                 # Coordinate conversion
#                 has_coordinates = st.radio(
#                     f"Does the dataset '{csv_file.name}' contain coordinates?",
#                     options=["Yes", "No"],
#                     index=1  # Default to "No"
#                 ) == "Yes"

#                 if has_coordinates:
#                     is_wgs84_coordinate = st.radio(
#                         f"Are the coordinates in '{csv_file.name}' already in WGS 84?",
#                         options=["Yes", "No"],
#                         index=0
#                     ) == "Yes"

#                     if not is_wgs84_coordinate:
#                         x_col = st.selectbox(f"Select the column for Longitude (x) in '{csv_file.name}':",
#                                              csv_file_data_frame.columns)
#                         y_col = st.selectbox(f"Select the column for Latitude (y) in '{csv_file.name}':",
#                                              csv_file_data_frame.columns)
#                         input_epsg = st.text_input(f"Enter the EPSG code of the input CRS (e.g., 3035):", "3035")

#                         if st.button(f"Transform Coordinates for {csv_file.name}"):
#                             try:
#                                 transformed_df = transform_coordinates(
#                                     csv_file_data_frame, x_col, y_col, f"EPSG:{input_epsg}", "EPSG:4326"
#                                 )
#                                 # Store the transformed DataFrame in session state
#                                 st.session_state[f"{csv_file.name}_data_frame"] = transformed_df
#                                 st.success(f"Coordinates in '{csv_file.name}' transformed to WGS 84!")
#                                 st.write(transformed_df.head())  # Preview the transformed DataFrame
#                             except Exception as e:
#                                 st.error(f"Error during transformation: {e}")
#                     else:
#                         # Save unchanged DataFrame to session state if not already saved
#                         if f"{csv_file.name}_data_frame" not in st.session_state:
#                             st.session_state[f"{csv_file.name}_data_frame"] = csv_file_data_frame

#                 ## retrieve the DataFrame from session state
#                 ##csv_file_data_frame = st.session_state.get(f"{csv_file.name}_data_frame", csv_file_data_frame)
#                 ##st.write(csv_file_data_frame.head())  # Display the DataFrame to ensure it is retained

#                 # Time dependency checks
#                 # column_headings = csv_file_data_frame.columns.tolist()
#                 # confirmed_matches = [key for key in mapping_json_file_paths if key in column_headings]
#                 is_time_dependant_csv = "date" in csv_file_data_frame.columns

#                 # is_time_dependant_csv = check_values_in_list(dictionary=time_data_json, lst=confirmed_matches)
#                 st.markdown(f"Is the dataset file '{csv_file.name}' time dependent?")

#                 col1, col2 = st.columns(2)
#                 with col1:
#                     if st.button(f"Confirm '{csv_file.name}' as time dependent"):
#                         st.session_state[f"{csv_file.name}_time_dependant"] = True
#                         is_time_dependant_csv = True

#                 with col2:
#                     if st.button(f"Confirm '{csv_file.name}' as time independent"):
#                         st.session_state[f"{csv_file.name}_time_dependant"] = False
#                         is_time_dependant_csv = False
#                         st.session_state[f"{csv_file.name}_parsing_method"] = None
#                         st.session_state[f"{csv_file.name}_date_col"] = None
#                         st.session_state[f"{csv_file.name}_time_col"] = None
#                         st.session_state[f"{csv_file.name}_date_format"] = None
#                         st.session_state[f"{csv_file.name}_time_format"] = None

#                 # Ensure session state assignment
#                 if f"{csv_file.name}_time_dependant" in st.session_state:
#                     is_time_dependant_csv = st.session_state[f"{csv_file.name}_time_dependant"]

#                 if is_time_dependant_csv:
#                     st.write(f"The file '{csv_file_name}' is time dependent.")
#                     # parsing_method, _, date_col, time_col, date_format, time_format, _ = time_with_each_column_user_input(
#                     #     time_data_json, csv_file, csv_content, csv_file_index, delimiter

#                     parsing_method = "single_date_column"
#                     date_col = "date"
#                     time_col = None
#                     date_format = "%Y-%m-%d"
#                     time_format = None
                    
#                     st.session_state[f"{csv_file.name}_parsing_method"] = parsing_method
#                     st.session_state[f"{csv_file.name}_date_col"] = date_col
#                     st.session_state[f"{csv_file.name}_time_col"] = time_col
#                     st.session_state[f"{csv_file.name}_date_format"] = date_format
#                     st.session_state[f"{csv_file.name}_time_format"] = time_format
#                 else:
#                     parsing_method = st.session_state.get(f"{csv_file.name}_parsing_method", None)
#                     date_col = st.session_state.get(f"{csv_file.name}_date_col", None)
#                     time_col = st.session_state.get(f"{csv_file.name}_time_col", None)
#                     date_format = st.session_state.get(f"{csv_file.name}_date_format", None)
#                     time_format = st.session_state.get(f"{csv_file.name}_time_format", None)

#                 # sensor_ID_column_heading = st.selectbox("Select the ID column of the sensor/measurement station:", csv_file_data_frame.columns)
#                 sensor_ID_column_heading = st.selectbox("Select the ID column of the sensor/measurement station:",csv_file_data_frame.columns,key=f"sensor_id_{csv_file.name}")


#                 # Use the DataFrame from session state
#                 csv_file_data_frame = st.session_state[f"{csv_file.name}_data_frame"]
#                 st.write(csv_file_data_frame.head())  # Verify DataFrame is retained

#                 if st.button(f"Convert {csv_file.name} to Turtle"):


#                     # to make the timers for the conversion process
#                     start_time = time.time()
#                     # --- CAMELS: extract gauge_id from filename ---
#                     if sensor_ID_column_heading not in csv_file_data_frame.columns:
#                         extracted_id = csv_file.name.split("_")[1].split(".")[0]
#                         csv_file_data_frame["ID"] = extracted_id
#                         sensor_ID_column_heading = "ID"

#                     result = process_csv_file(
#                         csv_file_data_frame, mapping_json_data, combined_prefixes_json_data, time_data_json,
#                         is_time_dependant_csv, gauge_or_catchment,sensor_ID_column_heading, csv_file_name, parsing_method, date_col, time_col, date_format, time_format
#                     )

#                     # stop the timer
#                     end_time = time.time()

#                     # time taken for the conversion
#                     elapsed_time_conversion = end_time - start_time


#                     if result:
#                         output_turtle_file, num_triples = result
#                         st.success(f"Turtle file created: {output_turtle_file}")
#                         st.download_button(label="Download Turtle file", data=open(output_turtle_file).read(), file_name=output_turtle_file)
#                         st.write(f"Elapsed time: {elapsed_time_conversion:.2f} seconds")
#                         st.write(f"Number of triples generated : {num_triples}")


#     elif file_type == "Shapefile":

#         shapefile = st.file_uploader(

#             "Upload a Shapefile (must include all necessary files, e.g., .shp, .dbf, .shx, .prj)", type=["zip"])

#         mapping_json_files = st.file_uploader("Upload Mapping JSON files", type=['json'], accept_multiple_files=True)

#         prefixes_json_files = st.file_uploader("Upload Prefixes JSON files", type=["json"], accept_multiple_files=True)

#         if shapefile and mapping_json_files and prefixes_json_files:

#             st.write("Processing shapefile...")

#             # Save the uploaded shapefile ZIP locally

#             shapefile_path = save_file(shapefile)

#             gdf = gpd.read_file(f"zip://{shapefile_path}")

#             # Ensure CRS is WGS 84

#             gdf = ensure_wgs84(gdf)

#             gdf['wkt'] = gdf['geometry'].apply(lambda geom: geom.wkt)

#             st.write("Shapefile loaded successfully:")

#             st.write(gdf.head())  # Preview GeoDataFrame

#             # Save mapping JSON files locally and load them

#             mapping_json_file_paths = [save_file(f) for f in mapping_json_files]

#             selected_mapping_json_file_path = st.selectbox("Select mapping JSON file for shapefile",
#                                                            mapping_json_file_paths)

#             mapping_json_data = load_json(selected_mapping_json_file_path)  # Load the selected JSON file

#             # Save prefixes JSON files locally and combine them

#             prefixes_json_file_paths = [save_file(f) for f in prefixes_json_files]

#             combined_prefixes_json_data = combine_json_files(prefixes_json_file_paths)

#             sensor_ID_column_heading = st.selectbox("Select the ID column of the sensor/measurement station:",
#                                                     gdf.columns)

#             parsing_method = None

#             date_col = None

#             time_col = None

#             date_format = None

#             time_format = None

#             if st.button(f"Convert Shapefile to Turtle"):

#                 start_time = time.time()

#                 is_time_dependant_file = False

#                 guage_or_catchment = None

#                 # Process shapefile data using process_csv_file function (adjusted for shapefiles)

#                 result = process_csv_file(

#                     gdf, mapping_json_data, combined_prefixes_json_data, None, is_time_dependant_file,

#                     guage_or_catchment, sensor_ID_column_heading, shapefile.name, parsing_method,

#                     date_col, time_col, date_format, time_format

#                 )

#                 end_time = time.time()

#                 elapsed_time_conversion = end_time - start_time

#                 if result:
#                     output_turtle_file, num_triples = result

#                     st.success(f"Turtle file created: {output_turtle_file}")

#                     st.download_button(label="Download Turtle file", data=open(output_turtle_file).read(),

#                                        file_name=output_turtle_file)

#                     st.write(f"Elapsed time: {elapsed_time_conversion:.2f} seconds")

#                     st.write(f"Number of triples generated: {num_triples}")


# def main():
#     st.sidebar.title("Navigation Panel")
#     page = st.sidebar.radio("Go to", ['Home', 'Settings', 'About'])

#     if page == "Home":
#         home_page()
#     elif page == "Settings":
#         st.write("Settings page")
#     elif page == "About":
#         st.write("About page")


# if __name__ == "__main__":
#     main()

