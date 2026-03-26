import re
import networkx as nx
from pathlib import Path
from rdflib import Namespace

from src.graph_loader import load_graph, get_default_graph_files


HYDRO = Namespace("http://example.org/hydro/ontology#")


def _extract_reach_id(uri: str):
    match = re.search(r"Reach_(.+)$", str(uri))
    return match.group(1) if match else None


class HydroNetwork:
    def __init__(self, ttl_file=None):
        files = get_default_graph_files()
        self.ttl_file = Path(ttl_file) if ttl_file else files["merged"]

        if not self.ttl_file.exists():
            raise FileNotFoundError(
                f"HydroKG file not found: {self.ttl_file}. "
                f"Run python scripts/build_hydrokg.py first."
            )

        self.g = load_graph(self.ttl_file)
        self.nx_graph = self._build_network()

    def _build_network(self):
        G = nx.DiGraph()

        for s, _, o in self.g.triples((None, HYDRO.flowsTo, None)):
            s_id = _extract_reach_id(str(s))
            o_id = _extract_reach_id(str(o))

            if s_id is None or o_id is None:
                continue

            G.add_edge(s_id, o_id)

        return G

    def summary(self):
        G = self.nx_graph
        return {
            "nodes": G.number_of_nodes(),
            "edges": G.number_of_edges(),
            "sources": len([n for n in G.nodes if G.in_degree(n) == 0]),
            "outlets": len([n for n in G.nodes if G.out_degree(n) == 0]),
        }

    def upstream_reaches(self, reach_id):
        if reach_id not in self.nx_graph:
            return []
        return list(self.nx_graph.predecessors(reach_id))

    def downstream_reaches(self, reach_id):
        if reach_id not in self.nx_graph:
            return []
        return list(self.nx_graph.successors(reach_id))

    def upstream_subgraph(self, reach_id):
        if reach_id not in self.nx_graph:
            return nx.DiGraph()

        upstream_nodes = nx.ancestors(self.nx_graph, reach_id)
        upstream_nodes.add(reach_id)
        return self.nx_graph.subgraph(upstream_nodes).copy()


if __name__ == "__main__":
    net = HydroNetwork()
    print(net.summary())