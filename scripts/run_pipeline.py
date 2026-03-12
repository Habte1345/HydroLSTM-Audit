import sys
from pathlib import Path

# ------------------------------------------------
# Add project root to Python path
# ------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# ------------------------------------------------
# Import pipeline modules
# ------------------------------------------------
from kg_builder.build_hydro_schema import build_schema
from kg_builder.build_static_basin_backbone import build_static_backbone
from kg_builder.build_topology import main as build_topology


def run():

    print("\nHydroKG Pipeline\n")

    # Step 1: Build ontology schema
    build_schema()

    # Step 2: Build static basin backbone
    build_static_backbone()

    # Step 3: Build topology
    build_topology()


if __name__ == "__main__":
    run()