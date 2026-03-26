
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
BASE_DIR = "F:\Github_repos\HydroLSTM-Audit"

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
