"""
RDF Utilities for HydroKG
Handles triple creation, prefix mapping, and TTL writing.
"""

from rdflib import Graph


# -----------------------------------------------------
# Triple creation from list template
# -----------------------------------------------------
def process_given_list(given_list, element, identification_no, value_from_csv, all_triples):
    previous_blank_node = None
    has_previous_blank_node = False

    for i, sublist in enumerate(given_list):

        if i == 0:
            if isinstance(sublist[-1], str) and sublist[-1].endswith("_"):
                temp_triple = [
                    element + str(identification_no),
                    sublist[0],
                    sublist[1] + str(identification_no),
                ]
                previous_blank_node = sublist[1] + str(identification_no)
                has_previous_blank_node = True
            else:
                temp_triple = [
                    element + str(identification_no),
                    sublist[0][0],
                    '"' + str(value_from_csv) + '"' + str(sublist[-1][0]),
                ]

        elif i == len(given_list) - 1:

            if len(sublist) == 1:
                temp_triple = [temp_triple[0], sublist[0], '"' + str(value_from_csv) + '"']

            elif isinstance(sublist[-1], list) and len(sublist[-1]) == 3:
                temp_triple = [
                    temp_triple[0],
                    sublist[0],
                    f"""[
{sublist[1][0][0]} {sublist[1][0][1]};
{sublist[1][1][0]} "{value_from_csv}"{sublist[1][1][1]};
{sublist[1][2][0]} {sublist[1][2][1]}
]""",
                ]

            elif len(sublist) == 2 and has_previous_blank_node:
                temp_triple = [
                    previous_blank_node,
                    sublist[0][0],
                    '"' + str(value_from_csv) + '"' + str(sublist[1][0]),
                ]

            else:
                temp_triple = [
                    temp_triple[0],
                    sublist[0][0],
                    '"' + str(value_from_csv) + '"' + str(sublist[1][0]),
                ]

        else:

            if isinstance(sublist[-1], str) and sublist[-1].endswith("_"):
                temp_triple = [
                    previous_blank_node,
                    sublist[0],
                    sublist[1] + str(identification_no),
                ]
                has_previous_blank_node = True

            elif previous_blank_node:
                temp_triple = [previous_blank_node, sublist[0], sublist[1]]

        all_triples.append(temp_triple)

        if isinstance(sublist[-1], str) and sublist[-1].endswith("_"):
            previous_blank_node = sublist[1] + str(identification_no)

    return all_triples


# -----------------------------------------------------
# Triple creation from simple string
# -----------------------------------------------------
def process_given_string(predicate, element, identification_no, value_from_csv, all_triples):
    triple = [element + str(identification_no), predicate, '"' + str(value_from_csv) + '"']
    all_triples.append(triple)
    return all_triples


# -----------------------------------------------------
# Remove duplicate triples
# -----------------------------------------------------
def filter_unique_triples(triples):
    unique = []
    seen = set()

    for t in triples:
        key = tuple(t)
        if key not in seen:
            unique.append(t)
            seen.add(key)

    return unique


# -----------------------------------------------------
# Write triples to TTL
# -----------------------------------------------------
def write_triples_to_ttl(triples, output_file):

    with open(output_file, "w") as f:

        for triple in triples:

            if len(triple) == 3:
                s, p, o = triple
                f.write(f"{s} {p} {o} .\n")


# -----------------------------------------------------
# Prefix mapping
# -----------------------------------------------------
def add_prefixes(graph: Graph, prefix_dict):

    for prefix, uri in prefix_dict.items():
        graph.bind(prefix, uri)

    return graph