from pathlib import Path
from rdflib import Graph, Namespace

from src.graph_loader import load_graph, get_default_graph_files


HYDRO = Namespace("http://example.org/hydro/ontology#")


class HydroKGQuery:
    def __init__(self, ttl_file=None):
        files = get_default_graph_files()
        self.ttl_file = Path(ttl_file) if ttl_file else files["merged"]

        if not self.ttl_file.exists():
            raise FileNotFoundError(
                f"HydroKG file not found: {self.ttl_file}. "
                f"Run python scripts/build_hydrokg.py first."
            )

        self.g = load_graph(self.ttl_file)

    def run_query(self, query: str):
        return self.g.query(query)

    def get_reach_count(self):
        query = """
        PREFIX hydro: <http://example.org/hydro/ontology#>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

        SELECT (COUNT(?reach) AS ?count)
        WHERE {
            ?reach rdf:type hydro:Reach .
        }
        """
        result = list(self.g.query(query))
        return int(result[0][0]) if result else 0

    def get_flowsTo_count(self):
        query = """
        PREFIX hydro: <http://example.org/hydro/ontology#>

        SELECT (COUNT(*) AS ?count)
        WHERE {
            ?s hydro:flowsTo ?o .
        }
        """
        result = list(self.g.query(query))
        return int(result[0][0]) if result else 0

    def get_downstream_reach(self, reach_id):
        query = f"""
        PREFIX hydro: <http://example.org/hydro/ontology#>

        SELECT ?downstream
        WHERE {{
            hydro:Reach_{reach_id} hydro:flowsTo ?downstream .
        }}
        """
        return [str(row[0]) for row in self.g.query(query)]

    def get_upstream_reaches(self, reach_id):
        query = f"""
        PREFIX hydro: <http://example.org/hydro/ontology#>

        SELECT ?upstream
        WHERE {{
            ?upstream hydro:flowsTo hydro:Reach_{reach_id} .
        }}
        """
        return [str(row[0]) for row in self.g.query(query)]

    def get_reach_attributes(self, reach_id):
        query = f"""
        PREFIX hydro: <http://example.org/hydro/ontology#>

        SELECT ?p ?o
        WHERE {{
            hydro:Reach_{reach_id} ?p ?o .
        }}
        """
        return [(str(row[0]), str(row[1])) for row in self.g.query(query)]


if __name__ == "__main__":
    kg = HydroKGQuery()
    print("Reach count:", kg.get_reach_count())
    print("flowsTo count:", kg.get_flowsTo_count())