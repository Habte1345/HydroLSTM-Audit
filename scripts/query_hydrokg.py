import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.kg_query import HydroKGQuery


def main():
    kg = HydroKGQuery()

    print("\nHydroKG Query Summary\n")
    print("Reach count:", kg.get_reach_count())
    print("flowsTo count:", kg.get_flowsTo_count())

    sample_reach_id = input("\nEnter a reach ID to inspect (example: one nhdplusid): ").strip()

    if sample_reach_id:
        print("\nAttributes:")
        for p, o in kg.get_reach_attributes(sample_reach_id):
            print(f"  {p} -> {o}")

        print("\nDownstream:")
        for d in kg.get_downstream_reach(sample_reach_id):
            print(f"  {d}")

        print("\nUpstream:")
        for u in kg.get_upstream_reaches(sample_reach_id):
            print(f"  {u}")


if __name__ == "__main__":
    main()