import json
from rdflib import Graph, Namespace, Literal, RDF, RDFS, OWL, URIRef
from rdflib.namespace import XSD

def build_ontology(json_filepath, output_filepath):
    # 1. Load the JSON glossary data
    with open(json_filepath, 'r') as file:
        data = json.load(file)

    # 2. Initialize the Graph and define Namespaces
    g = Graph()
    HYDRO = Namespace("http://example.org/hydro/ontology#")
    
    # Bind prefixes for cleaner output in the .ttl file
    g.bind("hydro", HYDRO)
    g.bind("owl", OWL)
    g.bind("rdfs", RDFS)
    g.bind("xsd", XSD)

    # Add general metadata about the ontology
    ontology_uri = URIRef("http://example.org/hydro/ontology")
    g.add((ontology_uri, RDF.type, OWL.Ontology))
    g.add((ontology_uri, RDFS.comment, Literal(data["metadata"]["description"])))

    # 3. Process and Add Classes
    for class_name, details in data["classes"].items():
        class_uri = HYDRO[class_name]
        g.add((class_uri, RDF.type, OWL.Class))
        g.add((class_uri, RDFS.comment, Literal(details["description"])))

    # 4. Process and Add Properties
    for prop_name, details in data["properties"].items():
        prop_uri = HYDRO[prop_name]

        # Determine if it is an ObjectProperty (links to another node) or DatatypeProperty (links to a literal value)
        if details["type"] == "object_property":
            g.add((prop_uri, RDF.type, OWL.ObjectProperty))
        elif details["type"] == "data_property":
            g.add((prop_uri, RDF.type, OWL.DatatypeProperty))

        # Assign Domain (what class this property belongs to)
        domains = details["domain"]
        if isinstance(domains, list):
            # If multiple domains (like Observation and Prediction), add each
            for d in domains:
                g.add((prop_uri, RDFS.domain, HYDRO[d]))
        else:
            g.add((prop_uri, RDFS.domain, HYDRO[domains]))

        # Assign Range (what type of value this property points to)
        prop_range = details["range"]
        if details["type"] == "object_property":
            g.add((prop_uri, RDFS.range, HYDRO[prop_range]))
        else:
            # Map standard data types to strict XSD types
            if prop_range == "float":
                g.add((prop_uri, RDFS.range, XSD.float))
            elif prop_range == "integer":
                g.add((prop_uri, RDFS.range, XSD.integer))
            elif prop_range == "string":
                g.add((prop_uri, RDFS.range, XSD.string))

        # Add definitions and custom audit annotations
        if "definition" in details:
            g.add((prop_uri, RDFS.comment, Literal(details["definition"])))
        if "audit_role" in details:
            # Custom annotation property for your framework's logic
            g.add((prop_uri, HYDRO.auditRole, Literal(details["audit_role"])))
        if "unit" in details:
            g.add((prop_uri, HYDRO.hasUnit, Literal(details["unit"])))

    # 5. Serialize the graph to a Turtle (.ttl) file
    g.serialize(destination=output_filepath, format="turtle")
    print(f"Ontology successfully saved to {output_filepath}")

# Execute the function
if __name__ == "__main__":
    # Ensure your JSON file is named 'glossaries.json' in the same directory
    build_ontology("glossaries.json", "hydro_schema.ttl")