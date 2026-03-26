import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.network_builder import HydroNetwork


def main():
    net = HydroNetwork()

    print("\nHydro river network summary\n")
    for k, v in net.summary().items():
        print(f"{k}: {v}")

    sample_reach_id = input("\nEnter a reach ID to inspect: ").strip()

    if sample_reach_id:
        print("\nImmediate upstream reaches:")
        print(net.upstream_reaches(sample_reach_id))

        print("\nImmediate downstream reaches:")
        print(net.downstream_reaches(sample_reach_id))


if __name__ == "__main__":
    main()