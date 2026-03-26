import sys
from pathlib import Path

# ------------------------------------------------
# Add project root to Python path
# ------------------------------------------------
# PROJECT_ROOT = Path("C:\\Users\\wooda\\Downloads\\nrt_project").resolve().parents[1]
PROJECT_ROOT = Path(__file__).resolve().parents[1]


if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# ------------------------------------------------
# Import pipeline modules
# ------------------------------------------------
from kg_builder.build_hydro_schema import build_schema
from kg_builder.build_static_basin_backbone import build_static_backbone
from kg_builder.build_topology import main as build_topology

from kg_builder.build_dynamic_timeseries import build_dynamic_timeseries
from auditing.build_lstm_predictions import build_predictions
from scripts.build_hydrokg import main as build_hydrokg

from kg_builder.build_hydro_schema import build_schema

# ------------------------------------------------
# Pipeline Runner
# ------------------------------------------------
def run():

    print("\n==============================")
    print("HydroKG Production Pipeline")
    print("==============================\n")

    # Step 1
    print("Step 1: Building Hydro Ontology Schema")
    build_schema()
    print("✔ Schema built\n")

    # Step 2
    print("Step 2: Building Static Basin Backbone")
    build_static_backbone()
    print("✔ Static backbone built\n")

    # Step 3
    print("Step 3: Building River Network Topology")
    build_topology()
    print("✔ Topology built\n")

    # Step 4
    print("Step 4: Building Dynamic Hydro Observations")
    build_dynamic_timeseries()
    print("✔ Dynamic KG built\n")

    # Step 5
    print("Step 5: Building LSTM Prediction KG")
    build_predictions()
    print("✔ LSTM predictions built\n")

    # Step 6
    print("Step 6: Merging Hydro Knowledge Graph")
    build_hydrokg()
    print("✔ Final HydroKG built\n")

    print("==============================")
    print("Pipeline completed successfully")
    print("==============================\n")


# ------------------------------------------------
# Entry point
# ------------------------------------------------
if __name__ == "__main__":
    run()