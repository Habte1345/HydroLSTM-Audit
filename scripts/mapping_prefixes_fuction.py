
# """
# import json

# def load_prefixes(file_path):
#     with open(file_path, 'r') as f:
#         prefixes = json.load(f)
#     return prefixes.get("prefixes", {})

# # function to remove special characters in the prefixes (specially in the last element of the triple)
# def extract_suffix(input_string):
#     special_chars = special_chars = [',', '~', '!', '@', '#', '$', '%', '^', '&', '*', '(', ')', '_', '-', '+', '=',
#                                      '{', '[', '}', '}', '|', '\\', ':', ';', '"', "'", '<', ',', '>', '.', '?', '/',
#                                      '1', '2', '3', '4', '5', '6', '7', '8', '9', '0']
#     for char in reversed(input_string):
#         if char in special_chars:
#             return input_string[input_string.rindex(char) + 1:]
#     return input_string


# def print_prefixes(prefixes_json, unique_triples_list):

#     # empty list of prefixes
#     prefixes_list_used = []

#     #taking all the prefixes used in the mapping of dataset
#     for triple_subset in unique_triples_list:

#         if len(triple_subset) == 3:
#             triple_1, triple_2, triple_3 = triple_subset[0], triple_subset[1], triple_subset[2]

#             # extract prefix from the terms used
#             prefix_1 = triple_1.split(':')[0]
#             prefix_2 = triple_2.split(':')[0]
#             prefix_3 = triple_3.split(':')[0]

#             # removing the special characters in triple_3
#             prefix_3 = extract_suffix(prefix_3)


#             # append all the identified prefixes to the prefixes list
#             prefixes_list_used.append(prefix_1)
#             prefixes_list_used.append(prefix_2)
#             prefixes_list_used.append(prefix_3)

#     # remove duplicates in prefix list
#     prefixes_list_used = list(dict.fromkeys(prefixes_list_used))

#     # loading prefixes from the prefixes JSON file
#     prefixes = load_prefixes(prefixes_json)

#     return prefixes_list_used , prefixes

#     # printing prefixes
# """
# import json
# import re

# def combine_json_files(json_file_paths):
#     combined_data = {}
#     for file_path in json_file_paths:
#         with open(file_path, 'r') as f:
#             data = json.load(f)
#             combined_data.update(data)
#     return combined_data


# # this function is to load the JSON file of prefixes to load and I chaged this function to use loaded JSON
# # as dictionary instead of directly JSON file
# # def load_prefixes(file_path):
# #     with open(file_path, 'r') as f:
# #         prefixes = json.load(f)
# #     return prefixes.get("prefixes", {})

# def load_prefixes(prefiexes_dictionary):
#     return prefiexes_dictionary.get("prefixes", {})

# # function to remove special characters in the prefixes (specially in the last element of the triple)
# def extract_suffix(input_string):
#     special_chars = special_chars = [',', '~', '!', '@', '#', '$', '%', '^', '&', '*', '(', ')', '_', '-', '+', '=',
#                                      '{', '[', '}', '}', '|', '\\', ':', ';', '"', "'", '<', ',', '>', '.', '?', '/',
#                                      '1', '2', '3', '4', '5', '6', '7', '8', '9', '0']
#     for char in reversed(input_string):
#         if char in special_chars:
#             return input_string[input_string.rindex(char) + 1:]
#     return input_string

# def extract_prefixes_from_string(input_string):
#     # Extract prefixes from a string that contains multiple prefixes
#     prefix_pattern = re.compile(r'\b([a-zA-Z0-9_]+):')
#     return prefix_pattern.findall(input_string)

# def print_prefixes(prefixes_json, unique_triples_list):
#     # Empty list of prefixes
#     prefixes_list_used = []

#     # Taking all the prefixes used in the mapping of dataset
#     for triple_subset in unique_triples_list:
#         if len(triple_subset) == 3:
#             triple_1, triple_2, triple_3 = triple_subset[0], triple_subset[1], triple_subset[2]

#             #print(triple_1, triple_2, triple_3) #debugging

#             # Extract prefixes from the terms used
#             prefixes_list_used.extend(extract_prefixes_from_string(triple_1))
#             prefixes_list_used.extend(extract_prefixes_from_string(triple_2))
#             prefixes_list_used.extend(extract_prefixes_from_string(triple_3))

#     # Remove duplicates in prefix list
#     prefixes_list_used = list(dict.fromkeys(prefixes_list_used))

#     # Loading prefixes from the prefixes JSON file
#     prefixes = load_prefixes(prefixes_json)

#     return prefixes_list_used, prefixes


# if __name__ == "__main__":
#     # Example usage
#     unique_triples_list = [
#         ['n4e_hyd:sensor_1', 'n4e_hyd:hasSaturatedHydraulicConductivity', 'n4e_hyd:saturatedhydraulicconductivity_1'],
#         ['n4e_hyd:saturatedhydraulicconductivity_1', 'rdf:type', 'envthes:22221'],
#         ['n4e_hyd:saturatedhydraulicconductivity_1', 'owl:hasValue', '[\n                    rdf:type  qudt:numericValue ;\n                    qudt:numericValue  "4668.379"^^xsd:decimal;\n                    qudt:unit  unit:CentiM-PER-HR]']
#     ]
#     prefixes_json = 'prefixes_dic.json'  # Adjust the path to your prefixes JSON file

#     prefixes_list_used, prefixes = print_prefixes(prefixes_json, unique_triples_list)

#     print("Prefixes used:", prefixes_list_used)
#     print("Prefixes loaded:", prefixes)


import json
import re

# ============================================================
# Load prefixes from already-loaded JSON dictionary
# ============================================================
def load_prefixes(prefixes_dictionary):
    """
    prefixes_dictionary must already be a dict loaded from JSON
    """
    return prefixes_dictionary.get("prefixes", {})


# ============================================================
# SAFE prefix extraction (CRITICAL FIX)
# ============================================================
def extract_prefix(term):
    """
    Extract prefix ONLY if term is a valid CURIE (prefix:suffix).

    This explicitly ignores:
    - Literals ("1981-01-01T00:00:00Z"^^xsd:dateTime)
    - Blank nodes ([ ... ])
    - Numeric strings
    """

    if not isinstance(term, str):
        return None

    term = term.strip()

    # Ignore literals
    if term.startswith('"'):
        return None

    # Ignore blank nodes
    if term.startswith('[') or term.startswith('_:'):
        return None

    # Ignore pure numbers
    if term.replace('.', '', 1).isdigit():
        return None

    # Match valid CURIE: prefix:suffix
    match = re.match(r'^([a-zA-Z_][a-zA-Z0-9_]*)\:.+$', term)
    if match:
        return match.group(1)

    return None


# ============================================================
# MAIN prefix detection function
# ============================================================
def print_prefixes(prefixes_json, unique_triples_list):
    """
    Returns:
        prefixes_list_used : list[str]
        prefixes           : dict
    """

    prefixes_set = set()

    for triple in unique_triples_list:
        if len(triple) != 3:
            continue

        subject, predicate, obj = triple

        for term in (subject, predicate, obj):
            prefix = extract_prefix(term)
            if prefix:
                prefixes_set.add(prefix)

    prefixes_list_used = sorted(prefixes_set)

    prefixes = load_prefixes(prefixes_json)

    return prefixes_list_used, prefixes


# ============================================================
# TEST
# ============================================================
if __name__ == "__main__":

    unique_triples_list = [
        ['n4e_hyd:observation_1', 'rdf:type', 'sosa:Observation'],
        ['n4e_hyd:observation_1', 'sosa:resultTime', '"1981-01-01T00:00:00Z"^^xsd:dateTime'],
        ['n4e_hyd:observation_1', 'sosa:hasResult',
         '[ rdf:type qudt:QuantityValue ; qudt:numericValue "1.23"^^xsd:decimal ; qudt:unit unit:MilliM ]']
    ]

    prefixes_json = {
        "prefixes": {
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
            "sosa": "http://www.w3.org/ns/sosa/",
            "xsd": "http://www.w3.org/2001/XMLSchema#",
            "qudt": "https://qudt.org/schema/qudt/",
            "unit": "https://qudt.org/vocab/unit/",
            "n4e_hyd": "https://nfdi4earth.pages.rwth-aachen.de/knowledgehub/nfdi4earth-ontology/test_hyd#"
        }
    }

    prefixes_list_used, prefixes = print_prefixes(prefixes_json, unique_triples_list)

    print("Prefixes used:", prefixes_list_used)
