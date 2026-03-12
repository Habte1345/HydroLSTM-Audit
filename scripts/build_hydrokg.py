import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.graph_loader import merge_hydrokg, graph_summary


def main():
    print("\nMerging HydroKG...\n")

    g, output_file = merge_hydrokg()

    print(f"HydroKG written to: {output_file}")
    print("Summary:")
    for k, v in graph_summary(g).items():
        print(f"  {k}: {v}")


if __name__ == "__main__":
    main()