"""
build_hydro_schema.py

Step 1 of Hydrologic Knowledge Graph construction.

Builds the ontology schema (T-Box) from glossaries.json
and writes hydro_schema.ttl.

This version preserves the original schema logic:
- metadata description
- multiple domains
- object/data property distinction
- proper range handling
- custom annotations: auditRole, hasUnit
"""

from pathlib import Path
import json
from rdflib import Graph, Namespace, Literal, RDF, RDFS, OWL, URIRef
from rdflib.namespace import XSD


# ----------------------------------------------------------
# CONFIGURATION
# ----------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[1]

INPUT_JSON = PROJECT_ROOT / "ontology" / "glossaries.json"
OUTPUT_TTL = PROJECT_ROOT / "ontology" / "hydro_schema.ttl"

HYDRO = Namespace("http://example.org/hydro/ontology#")
ONTOLOGY_URI = URIRef("http://example.org/hydro/ontology")


# ----------------------------------------------------------
# BUILD ONTOLOGY
# ----------------------------------------------------------

def build_ontology(json_filepath=INPUT_JSON, output_filepath=OUTPUT_TTL):
    # 1. Load the JSON glossary data
    with open(json_filepath, "r", encoding="utf-8") as file:
        data = json.load(file)

    # 2. Initialize the Graph and define Namespaces
    g = Graph()

    # Bind prefixes for cleaner output in the .ttl file
    g.bind("hydro", HYDRO)
    g.bind("owl", OWL)
    g.bind("rdfs", RDFS)
    g.bind("xsd", XSD)

    # Add general metadata about the ontology
    g.add((ONTOLOGY_URI, RDF.type, OWL.Ontology))

    if "metadata" in data and "description" in data["metadata"]:
        g.add((ONTOLOGY_URI, RDFS.comment, Literal(data["metadata"]["description"])))

    # Explicitly declare custom annotation properties used in the ontology
    g.add((HYDRO.auditRole, RDF.type, OWL.AnnotationProperty))
    g.add((HYDRO.hasUnit, RDF.type, OWL.AnnotationProperty))

    # 3. Process and Add Classes
    for class_name, details in data.get("classes", {}).items():
        class_uri = HYDRO[class_name]
        g.add((class_uri, RDF.type, OWL.Class))

        if "description" in details:
            g.add((class_uri, RDFS.comment, Literal(details["description"])))

    # 4. Process and Add Properties
    for prop_name, details in data.get("properties", {}).items():
        prop_uri = HYDRO[prop_name]

        prop_type = details.get("type", None)

        # Determine if it is an ObjectProperty or DatatypeProperty
        if prop_type == "object_property":
            g.add((prop_uri, RDF.type, OWL.ObjectProperty))
        elif prop_type == "data_property":
            g.add((prop_uri, RDF.type, OWL.DatatypeProperty))
        else:
            # safe fallback
            g.add((prop_uri, RDF.type, OWL.DatatypeProperty))

        # Assign Domain
        domains = details.get("domain", None)
        if isinstance(domains, list):
            for d in domains:
                g.add((prop_uri, RDFS.domain, HYDRO[d]))
        elif isinstance(domains, str):
            g.add((prop_uri, RDFS.domain, HYDRO[domains]))

        # Assign Range
        prop_range = details.get("range", None)

        if prop_type == "object_property":
            if isinstance(prop_range, str):
                g.add((prop_uri, RDFS.range, HYDRO[prop_range]))
        else:
            if prop_range == "float":
                g.add((prop_uri, RDFS.range, XSD.float))
            elif prop_range == "integer":
                g.add((prop_uri, RDFS.range, XSD.integer))
            elif prop_range == "string":
                g.add((prop_uri, RDFS.range, XSD.string))
            elif prop_range == "dateTime":
                g.add((prop_uri, RDFS.range, XSD.dateTime))
            elif prop_range == "date":
                g.add((prop_uri, RDFS.range, XSD.date))
            elif prop_range == "boolean":
                g.add((prop_uri, RDFS.range, XSD.boolean))

        # Add definitions and custom audit annotations
        if "definition" in details:
            g.add((prop_uri, RDFS.comment, Literal(details["definition"])))

        if "audit_role" in details:
            g.add((prop_uri, HYDRO.auditRole, Literal(details["audit_role"])))

        if "unit" in details:
            g.add((prop_uri, HYDRO.hasUnit, Literal(details["unit"])))

    # 5. Serialize the graph to a Turtle (.ttl) file
    output_filepath = Path(output_filepath)
    output_filepath.parent.mkdir(parents=True, exist_ok=True)

    g.serialize(destination=str(output_filepath), format="turtle")

    print(f"Ontology successfully saved to {output_filepath}")
    print(f"Total triples: {len(g)}")

    return g


# ----------------------------------------------------------
# WRAPPER FOR PIPELINE
# ----------------------------------------------------------

def build_schema():
    return build_ontology(INPUT_JSON, OUTPUT_TTL)


# ----------------------------------------------------------
# RUN
# ----------------------------------------------------------

if __name__ == "__main__":
    build_schema()