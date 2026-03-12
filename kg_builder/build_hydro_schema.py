"""
Build HydroKG Ontology Schema (T-Box)
"""

from pathlib import Path
import json
from rdflib import Graph, Namespace, RDF, RDFS, OWL, Literal
from rdflib.namespace import XSD


def build_schema():

    PROJECT_ROOT = Path(__file__).resolve().parents[1]

    ontology_file = PROJECT_ROOT / "ontology" / "glossaries.json"
    output_file = PROJECT_ROOT / "ontology" / "hydro_schema.ttl"

    HYDRO = Namespace("https://alabama-hydro.org/ontology/")

    print("Loading glossary file:", ontology_file)

    with open(ontology_file, "r") as f:
        data = json.load(f)

    g = Graph()

    g.bind("hydro", HYDRO)
    g.bind("owl", OWL)
    g.bind("rdfs", RDFS)
    g.bind("xsd", XSD)

    ontology_uri = HYDRO["HydroOntology"]

    g.add((ontology_uri, RDF.type, OWL.Ontology))

    # --------------------------------------------------
    # Classes
    # --------------------------------------------------

    classes = data.get("classes", {})

    for class_name, details in classes.items():

        class_uri = HYDRO[class_name]

        g.add((class_uri, RDF.type, OWL.Class))

        description = details.get("description", "")

        if description:
            g.add((class_uri, RDFS.comment, Literal(description)))

    # --------------------------------------------------
    # Properties
    # --------------------------------------------------

    properties = data.get("properties", {})

    for prop_name, details in properties.items():

        prop_uri = HYDRO[prop_name]

        ptype = details.get("type", "data_property")

        if ptype == "object_property":
            g.add((prop_uri, RDF.type, OWL.ObjectProperty))
        else:
            g.add((prop_uri, RDF.type, OWL.DatatypeProperty))

        domain = details.get("domain")

        if domain:
            g.add((prop_uri, RDFS.domain, HYDRO[domain]))

        prange = details.get("range")

        if prange == "float":
            g.add((prop_uri, RDFS.range, XSD.float))

        elif prange == "integer":
            g.add((prop_uri, RDFS.range, XSD.integer))

        elif prange == "string":
            g.add((prop_uri, RDFS.range, XSD.string))

        definition = details.get("definition")

        if definition:
            g.add((prop_uri, RDFS.comment, Literal(definition)))

    g.serialize(destination=output_file, format="turtle")

    print("Schema written to:", output_file)
    print("Total triples:", len(g))