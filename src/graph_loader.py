from pathlib import Path
from rdflib import Graph


PROJECT_ROOT = Path(__file__).resolve().parents[1]
ONTOLOGY_DIR = PROJECT_ROOT / "ontology"
RDF_DIR = PROJECT_ROOT / "rdf"


def get_default_graph_files():
    return {
        "schema": ONTOLOGY_DIR / "hydro_schema.ttl",
        "static": RDF_DIR / "basin_static.ttl",
        "topology": RDF_DIR / "basin_topology.ttl",
        "dynamic": RDF_DIR / "dynamic_streamflow.ttl",
        "predictions": RDF_DIR / "lstm_predictions.ttl",
        "merged": RDF_DIR / "hydroKG.ttl",
    }



def load_graph(*ttl_files):
    g = Graph()

    for ttl_file in ttl_files:
        if ttl_file is None:
            continue

        ttl_path = Path(ttl_file)

        if not ttl_path.exists():
            raise FileNotFoundError(f"TTL file not found: {ttl_path}")

        g.parse(str(ttl_path), format="turtle")

    return g


def merge_hydrokg(
    schema_file=None,
    static_file=None,
    topology_file=None,
    dynamic_file=None,
    pred_file=None,
    output_file=None,
):

    files = get_default_graph_files()

    schema_file = Path(schema_file) if schema_file else files["schema"]
    static_file = Path(static_file) if static_file else files["static"]
    topology_file = Path(topology_file) if topology_file else files["topology"]
    dynamic_file = Path(dynamic_file) if dynamic_file else files["dynamic"]
    pred_file = Path(pred_file) if pred_file else files["predictions"]
    output_file = Path(output_file) if output_file else files["merged"]

    # --------------------------------------------------
    # Build list of graphs to merge
    # --------------------------------------------------

    graph_files = [schema_file, static_file, topology_file]

    if dynamic_file.exists():
        graph_files.append(dynamic_file)

    if pred_file.exists():
        graph_files.append(pred_file)

    # --------------------------------------------------
    # Load and merge graphs
    # --------------------------------------------------

    g = load_graph(*graph_files)

    output_file.parent.mkdir(parents=True, exist_ok=True)

    g.serialize(destination=str(output_file), format="turtle")

    return g, output_file


def graph_summary(g: Graph):
    return {
        "triples": len(g),
        "subjects": len(set(g.subjects())),
        "predicates": len(set(g.predicates())),
        "objects": len(set(g.objects())),
    }


if __name__ == "__main__":
    g, out = merge_hydrokg()
    print(f"Merged HydroKG saved to: {out}")
    print(graph_summary(g))
