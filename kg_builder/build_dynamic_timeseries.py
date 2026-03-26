import sys
from pathlib import Path
import pandas as pd

from rdflib import Graph, Namespace, Literal
from rdflib.namespace import RDF, XSD


# --------------------------------------------------
# Paths
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[1]

CARAVAN_DIR = Path(r"E:\Data\Caravan\Caravan\timeseries\csv\camels")

BASIN_LIST_FILE = Path(
    r"C:\Users\hdagne1\Box\NRT_Project_2026_Spring\Habtamu\HydroAuditToolFrameowrk\data\basin_list.txt"
)

OUTPUT_TTL = PROJECT_ROOT / "rdf" / "dynamic_streamflow.ttl"

HYDRO = Namespace("http://example.org/hydro/ontology#")


# --------------------------------------------------
# Columns needed
# --------------------------------------------------

SELECTED_COLUMNS = [
    "date",
    "total_precipitation_sum",
    "potential_evaporation_sum",
    "temperature_2m_mean",
    "surface_net_solar_radiation_mean",
    "volumetric_soil_water_layer_1_mean",
    "volumetric_soil_water_layer_2_mean",
    "volumetric_soil_water_layer_3_mean",
    "streamflow",
]


# --------------------------------------------------
# Load basin list
# --------------------------------------------------

def load_basin_list():
    with open(BASIN_LIST_FILE, "r", encoding="utf-8") as f:
        basins = [x.strip() for x in f if x.strip()]

    print(f"Loaded {len(basins)} basins")
    return basins


# --------------------------------------------------
# Build dynamic KG
# --------------------------------------------------

def build_dynamic_timeseries():
    basins = load_basin_list()

    g = Graph()
    g.bind("hydro", HYDRO)

    created_timesteps = set()
    created_catchments = set()
    total_rows = 0

    for basin in basins:
        csv_file = CARAVAN_DIR / f"camels_{basin}.csv"

        if not csv_file.exists():
            print("Missing:", csv_file)
            continue

        print("Processing basin:", basin)

        df = pd.read_csv(csv_file, usecols=SELECTED_COLUMNS)

        catchment_uri = HYDRO[f"Catchment_{basin}"]

        # Create catchment node once
        if basin not in created_catchments:
            g.add((catchment_uri, RDF.type, HYDRO.Catchment))
            created_catchments.add(basin)

        for i, row in enumerate(df.itertuples(index=False)):
            date_str = str(row.date)

            obs_uri = HYDRO[f"Observation_{basin}_{i}"]
            ts_uri = HYDRO[f"TimeStep_{date_str}"]

            # Observation node
            g.add((obs_uri, RDF.type, HYDRO.Observation))
            g.add((obs_uri, HYDRO.forCatchment, catchment_uri))

            # TimeStep node (create once)
            if date_str not in created_timesteps:
                g.add((ts_uri, RDF.type, HYDRO.TimeStep))
                created_timesteps.add(date_str)

            g.add((obs_uri, HYDRO.hasTimeStep, ts_uri))

            # Dynamic attributes
            if pd.notna(row.total_precipitation_sum):
                g.add((
                    obs_uri,
                    HYDRO.hasPrecipitation,
                    Literal(float(row.total_precipitation_sum), datatype=XSD.float)
                ))

            if pd.notna(row.potential_evaporation_sum):
                g.add((
                    obs_uri,
                    HYDRO.hasPotentialEvaporation,
                    Literal(float(row.potential_evaporation_sum), datatype=XSD.float)
                ))

            if pd.notna(row.temperature_2m_mean):
                g.add((
                    obs_uri,
                    HYDRO.hasAirTemperature,
                    Literal(float(row.temperature_2m_mean), datatype=XSD.float)
                ))

            if pd.notna(row.surface_net_solar_radiation_mean):
                g.add((
                    obs_uri,
                    HYDRO.hasSolarRadiation,
                    Literal(float(row.surface_net_solar_radiation_mean), datatype=XSD.float)
                ))

            if pd.notna(row.volumetric_soil_water_layer_1_mean):
                g.add((
                    obs_uri,
                    HYDRO.hasSoilMoistureL1,
                    Literal(float(row.volumetric_soil_water_layer_1_mean), datatype=XSD.float)
                ))

            if pd.notna(row.volumetric_soil_water_layer_2_mean):
                g.add((
                    obs_uri,
                    HYDRO.hasSoilMoistureL2,
                    Literal(float(row.volumetric_soil_water_layer_2_mean), datatype=XSD.float)
                ))

            if pd.notna(row.volumetric_soil_water_layer_3_mean):
                g.add((
                    obs_uri,
                    HYDRO.hasSoilMoistureL3,
                    Literal(float(row.volumetric_soil_water_layer_3_mean), datatype=XSD.float)
                ))

            if pd.notna(row.streamflow):
                g.add((
                    obs_uri,
                    HYDRO.hasDischarge,
                    Literal(float(row.streamflow), datatype=XSD.float)
                ))

            total_rows += 1

    OUTPUT_TTL.parent.mkdir(parents=True, exist_ok=True)
    g.serialize(destination=str(OUTPUT_TTL), format="turtle")

    print("\nDynamic KG saved to:", OUTPUT_TTL)
    print("Total observations:", total_rows)
    print("Total triples:", len(g))

    return g


# --------------------------------------------------
# Run
# --------------------------------------------------

if __name__ == "__main__":
    build_dynamic_timeseries()
