import json
from pathlib import Path
from rdflib import Graph, Namespace, Literal, RDF, RDFS, OWL, URIRef
from rdflib.namespace import XSD

# --------------------------------------------------
# Paths
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[1]

INPUT_JSON = PROJECT_ROOT / "ontology" / "glossaries.json"
OUTPUT_TTL = PROJECT_ROOT / "ontology" / "hydro_schema.ttl"


# --------------------------------------------------
# Core ontology builder
# --------------------------------------------------

def build_ontology(json_filepath, output_filepath):

    with open(json_filepath, "r") as file:
        data = json.load(file)

    g = Graph()

    HYDRO = Namespace("http://example.org/hydro/ontology#")

    g.bind("hydro", HYDRO)
    g.bind("owl", OWL)
    g.bind("rdfs", RDFS)
    g.bind("xsd", XSD)

    ontology_uri = URIRef("http://example.org/hydro/ontology")

    g.add((ontology_uri, RDF.type, OWL.Ontology))
    g.add((ontology_uri, RDFS.comment, Literal(data["metadata"]["description"])))

    # --------------------------------------------------
    # Classes
    # --------------------------------------------------

    for class_name, details in data["classes"].items():

        class_uri = HYDRO[class_name]

        g.add((class_uri, RDF.type, OWL.Class))
        g.add((class_uri, RDFS.comment, Literal(details["description"])))

    # --------------------------------------------------
    # Properties
    # --------------------------------------------------

    for prop_name, details in data["properties"].items():

        prop_uri = HYDRO[prop_name]

        if details["type"] == "object_property":
            g.add((prop_uri, RDF.type, OWL.ObjectProperty))

        elif details["type"] == "data_property":
            g.add((prop_uri, RDF.type, OWL.DatatypeProperty))

        # Domain
        domains = details["domain"]

        if isinstance(domains, list):
            for d in domains:
                g.add((prop_uri, RDFS.domain, HYDRO[d]))
        else:
            g.add((prop_uri, RDFS.domain, HYDRO[domains]))

        # Range
        prop_range = details["range"]

        if details["type"] == "object_property":

            g.add((prop_uri, RDFS.range, HYDRO[prop_range]))

        else:

            if prop_range == "float":
                g.add((prop_uri, RDFS.range, XSD.float))
            elif prop_range == "integer":
                g.add((prop_uri, RDFS.range, XSD.integer))
            elif prop_range == "string":
                g.add((prop_uri, RDFS.range, XSD.string))
            elif prop_range == "date":
                g.add((prop_uri, RDFS.range, XSD.date))


        # Definitions
        if "definition" in details:
            g.add((prop_uri, RDFS.comment, Literal(details["definition"])))

        if "audit_role" in details:
            g.add((prop_uri, HYDRO.auditRole, Literal(details["audit_role"])))

        if "unit" in details:
            g.add((prop_uri, HYDRO.hasUnit, Literal(details["unit"])))

    g.serialize(destination=str(output_filepath), format="turtle")

    print("Ontology written to:", output_filepath)
    print("Total triples:", len(g))

    return g


# --------------------------------------------------
# Pipeline wrapper (required by run_pipeline.py)
# --------------------------------------------------

def build_schema():

    return build_ontology(INPUT_JSON, OUTPUT_TTL)


# --------------------------------------------------
# Run standalone
# --------------------------------------------------

if __name__ == "__main__":

    build_schema()
