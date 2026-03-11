# to trite triples to a Turtle (.ttl) file.
import mapping_prefixes_fuction

#
# def write_triples_to_turtle(unique_triples_list, prefixes_json, output_file_name):
#
#     # unique_triples_list: A list of triples after deletion of duplicates,
#     # filename: The name of the file to write to.
#
#     with open(output_file_name, 'w') as f:
#
#         # loading variables from prefixes mapping function
#         prefixes_list_used, prefixes = mapping_prefixes_fuction.print_prefixes(prefixes_json, unique_triples_list)
#
#         # printing prefixes
#
#         for prefix in prefixes_list_used:
#             if prefix in prefixes:
#                 f.write(f"@prefix {prefix}: <{prefixes[prefix]}> . \n")
#
#             else:
#                 print(f"Prefix '{prefix}' not found in the Prefixes dictionary JSON file.")
#
#         print(prefixes_list_used)
#
#         # print triples
#         for triple_subset in unique_triples_list:
#             if len(triple_subset) == 3:
#
#                 triple_1 = triple_subset[0]
#                 triple_2 = triple_subset[1]
#                 triple_3 = triple_subset[2]
#
#                 # Writing the triple to the file in Turtle format
#                 f.write(f"{triple_1} {triple_2} {triple_3} .\n")

### new function on 2024_05_01
import os
import mapping_prefixes_fuction

def write_triples_to_turtle(unique_triples_list, prefixes_json, output_file_name):

    # Load prefixes
    prefixes_list_used, prefixes = mapping_prefixes_fuction.print_prefixes(
        prefixes_json, unique_triples_list
    )

    # Derive ontology name from filename
    ontology_name = os.path.splitext(os.path.basename(output_file_name))[0]

    # Group triples by subject
    triples_by_subject = {}
    for triple in unique_triples_list:
        if len(triple) == 3:
            subject, predicate, obj = triple
            triples_by_subject.setdefault(subject, []).append((predicate, obj))

    with open(output_file_name, 'w', encoding='utf-8') as f:

        # write ALL prefixes from JSON
        for prefix, uri in prefixes_json.items():
            f.write(f"@prefix {prefix}: <{uri}> .\n")

        f.write("\n")

        f.write("@prefix : <https://nfdi4earth.pages.rwth-aachen.de/knowledgehub/nfdi4earth-ontology/test_hyd#> .\n")
        f.write("@prefix owl: <http://www.w3.org/2002/07/owl#> .\n\n")

        # --------------------------------------------------
        # ONTOLOGY DECLARATION (CRITICAL)
        # --------------------------------------------------
        f.write(f":{ontology_name} a owl:Ontology .\n\n")

        # --------------------------------------------------
        # RDF TRIPLES
        # --------------------------------------------------
        for subject, po_list in triples_by_subject.items():
            f.write(f"{subject} ")
            for i, (predicate, obj) in enumerate(po_list):
                if i == len(po_list) - 1:
                    f.write(f"{predicate} {obj} .\n")
                else:
                    f.write(f"{predicate} {obj} ;\n\t")

    print(prefixes_list_used)



if __name__ == '__main__':
    unique_triples_list = [
        ['n4e_hyd:sensor_1', 'n4e_hyd:govnrID', '"200014"'],
        ['n4e_hyd:sensor_1', 'schema:name', '"Bangs"'],
        ['n4e_hyd:sensor_1', 'dbo:river', '"Rhein"']
    ]
    prefixes_json = 'prefixes.json'  # Adjust the path to your prefixes JSON file
    output_file_name = 'output_test______.ttl'

    write_triples_to_turtle(unique_triples_list, prefixes_json, output_file_name)

