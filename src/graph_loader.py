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
        "merged": RDF_DIR / "hydroKG.ttl",
    }


def load_graph(*ttl_files):
    g = Graph()

    for ttl_file in ttl_files:
        ttl_path = Path(ttl_file)
        if not ttl_path.exists():
            raise FileNotFoundError(f"TTL file not found: {ttl_path}")
        g.parse(str(ttl_path), format="turtle")

    return g


def merge_hydrokg(
    schema_file=None,
    static_file=None,
    topology_file=None,
    output_file=None,
):
    files = get_default_graph_files()

    schema_file = Path(schema_file) if schema_file else files["schema"]
    static_file = Path(static_file) if static_file else files["static"]
    topology_file = Path(topology_file) if topology_file else files["topology"]
    output_file = Path(output_file) if output_file else files["merged"]

    g = load_graph(schema_file, static_file, topology_file)

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