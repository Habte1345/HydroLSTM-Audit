# from datetime import datetime
#
# def observation_mapping (given_list, observation_date_time ,sensor_ID, all_triples, observation_definition,
#                         result_time_term,  has_result_term, has_sim_result_term,
#                         observation_number ,value_from_csv):
#
# ## need to add when there is an sum, max, min, avg observations
#
#     if isinstance(given_list, list):
#         #iterate over the sub items in JSON vocabulary for relavant key
#         for i, sublist in enumerate(given_list):
#
#             if sublist[0] == result_time_term:
#                 #print(f"{i}, a date and time {observation_date_time}")
#                 temp_triple = [observation_definition + str(observation_number), sublist[0],
#                                f'"{observation_date_time.year}-{observation_date_time.month}-{observation_date_time.day}'
#                                f'T{observation_date_time.hour}:{observation_date_time.minute}:{observation_date_time.second}'
#                                f'Z"^^{sublist[-1]}']
#
#             # when the simple result is not a numerical value
#             elif sublist[0] == has_sim_result_term : #and isinstance(sublist, str):
#                 temp_triple = [observation_definition + str(observation_number), sublist[0], '"' +str(value_from_csv) + '"']
#                 #print(f"{i}, a simple results without decimal")
#
#             # when the simple result is a numerical value
#             elif sublist[0][0] == has_sim_result_term:  #and isinstance(sublist, list) and isinstance(sublist[1], list) :
#                 temp_triple = [observation_definition + str(observation_number), sublist[0][0], '"' +str(value_from_csv) + '"' + sublist[1][0] ]
#                 #print(f"{i}, a simple results with decimal")
#
#             # when result is with units
#             elif sublist[0] == has_result_term and isinstance(sublist, list) and isinstance(sublist[1], list) :
#                 #print("Result")
#                 temp_triple = [observation_definition+ str(observation_number), sublist[0],f"""[
#                     {sublist[1][0][0]}  {sublist[1][0][1]};
#                     {sublist[1][1][0]}  {str(value_from_csv)}{sublist[1][1][1]};
#                     {sublist[1][2][0]}  {sublist[1][2][1]}]"""]
#
#             # when the blank nodes are there in results expect for the results
#             elif sublist[0] != has_result_term and isinstance(sublist[-1], str) and sublist[-1].endswith("_"):
#                 #print(f"{i}, when the blank node is {sublist[-1]}")
#                 temp_triple = [observation_definition + str(observation_number), sublist[0], sublist[-1]+str(sensor_ID)]
#
#             # making triples considering only the given two terms in the JSON sublist
#             elif isinstance(sublist[-1],str) and not sublist[-1].endswith("_"):
#                 #print(f"{i}, without blanknodes {sublist[-1]}")
#                 temp_triple = [observation_definition + str(observation_number), sublist[0], sublist[-1]]
#
#             else:
#                 #print("not working",f"for {sublist[-1]}")
#                 temp_triple = []
#
#             all_triples.append(temp_triple)
#
#
#
# if __name__ == "__main__":
#     given_list = [["sosa:observedProperty", "n4e_hyd:AirTemperature2m"],
#                      ["sosa:hasFeatureOfInterest", "n4e_hyd:catchment_" ],
#                      ["sosa:madeBySensor", "n4e_hyd:sensor_"],
#                      ["sosa:resultTime","xsd:dateTime"],
#                      ["sosa:hasResult", "n4e_hyd:temperature2mResult_"],
#                      ["rdf:type","sosa:Result"],
#                      ["cpmeta:hasMaxValue",
#                        [["rdf:type","qudt:numericValue "],
#                       ["qudt:numericValue","^^xsd:decimal"],
#                       ["qudt:unit","unit:DEG_C"]]]]
#
#
#     #element = json_data['observation_designation'][0]
#     value_from_csv = 1.2334
#     has_result_term = "sosa:hasResult"
#     has_sim_result_term = "sosa:hasSimpleResult"
#     result_time_term = "sosa:resultTime"
#     observation_definition = "n4e_hyd:observation_"
#     made_by_sensor_term = "sosa:madeBySensor"
#     number_of_obervations_per_row = 10
#     observation_date_time = datetime(year= 2000, month= 10, day= 14, hour= 11, minute= 9, second= 9)
#     sensor_ID = 1
#     all_triples = []
#     observation_no = 2
#
#     observation_maping(given_list, observation_date_time, sensor_ID, all_triples, observation_definition,result_time_term, has_result_term, has_sim_result_term, observation_no, value_from_csv)
#
#     for triple_subset in all_triples:
#         if len(triple_subset) == 3:
#             triple_1 = triple_subset[0]
#             triple_2 = triple_subset[1]
#             triple_3 = triple_subset[2]
#
#             # Writing the triple to the file in Turtle format
#             print(f"{triple_1} {triple_2} {triple_3} .")
#
#


### new function 2023_05_21
#
# from datetime import datetime
#
# def observation_mapping(given_list, observation_date_time, sensor_ID, all_triples, observation_definition,
#                         result_time_term, has_result_term, has_sim_result_term,
#                         observation_number, value_from_csv):
#     if isinstance(given_list, list):
#         for i, sublist in enumerate(given_list):
#             # Handling result time
#             if sublist[0] == result_time_term:
#                 temp_triple = [
#                     observation_definition + str(observation_number), sublist[0],
#                     f'"{observation_date_time.year}-{observation_date_time.month:02d}-{observation_date_time.day:02d}'
#                     f'T{observation_date_time.hour:02d}:{observation_date_time.minute:02d}:{observation_date_time.second:02d}'
#                     f'Z"^^{sublist[-1]}'
#                 ]
#             # Handling simple result
#             elif sublist[0] == has_sim_result_term:
#                 temp_triple = [
#                     observation_definition + str(observation_number), sublist[0], '"' + str(value_from_csv) + '"'
#                 ]
#             # Handling simple result with nested structure
#             elif isinstance(sublist[0], list) and sublist[0][0] == has_sim_result_term and isinstance(sublist[1], list):
#                 temp_triple = [
#                     observation_definition + str(observation_number), sublist[0][0], '"' + str(value_from_csv) + '"' + sublist[1][0]
#                 ]
#             # Handling complex results with units
#             elif sublist[0] == has_result_term and isinstance(sublist[1], list):
#                 temp_triple = [
#                     observation_definition + str(observation_number), sublist[0],
#                     f"""[
#                     {sublist[1][0][0]}  {sublist[1][0][1]};
#                     {sublist[1][1][0]}  "{str(value_from_csv)}"{sublist[1][1][1]};
#                     {sublist[1][2][0]}  {sublist[1][2][1]}]"""
#                 ]
#             # Handling nested lists like hasMaxValue
#             elif isinstance(sublist[1], list) and isinstance(sublist[1][0], list):
#                 value_node = f"""
#                     {sublist[1][0][0]}  {sublist[1][0][1]};
#                     {sublist[1][1][0]}  "{str(value_from_csv)}"{sublist[1][1][1]};
#                     {sublist[1][2][0]}  {sublist[1][2][1]}
#                 """
#                 temp_triple = [
#                     observation_definition + str(observation_number), sublist[0], f"""[
#                     {value_node.strip()}]"""
#                 ]
#             # Handling blank nodes
#             elif sublist[0] != has_result_term and isinstance(sublist[-1], str) and sublist[-1].endswith("_"):
#                 temp_triple = [
#                     observation_definition + str(observation_number), sublist[0], sublist[-1] + str(sensor_ID)
#                 ]
#             # Handling other cases
#             elif isinstance(sublist[-1], str) and not sublist[-1].endswith("_"):
#                 temp_triple = [
#                     observation_definition + str(observation_number), sublist[0], sublist[-1]
#                 ]
#             else:
#                 temp_triple = []
#
#             if temp_triple:
#                 all_triples.append(temp_triple)
#
#
# if __name__ == "__main__":
#     # given_list = [["sosa:observedProperty", "n4e_hyd:ForecastAlbedo"],
#     #                  ["sosa:hasFeatureOfInterest", "n4e_hyd:catchment_" ],
#     #                  ["sosa:madeBySensor", "n4e_hyd:sensor_"],
#     #                  ["sosa:resultTime","xsd:dateTime"],
#     #                  [["sosa:hasSimpleResult" ], ["^^xsd:decimal"]]]
#
#     given_list = [["sosa:observedProperty", "n4e_hyd:AirTemperature2m"],
#                      ["sosa:hasFeatureOfInterest", "n4e_hyd:catchment_" ],
#                      ["sosa:madeBySensor", "n4e_hyd:sensor_"],
#                      ["sosa:resultTime","xsd:dateTime"],
#                      ["sosa:hasResult", "n4e_hyd:temperature2mResult_"],
#                      ["rdf:type","sosa:Result"],
#                      ["cpmeta:hasMaxValue",
#                        [["rdf:type","qudt:QuantityValue "],
#                       ["qudt:numericValue","^^xsd:decimal"],
#                       ["qudt:unit","unit:DEG_C"]]]]
#
#     value_from_csv = 1.2334
#     has_result_term = "sosa:hasResult"
#     has_sim_result_term = "sosa:hasSimpleResult"
#     result_time_term = "sosa:resultTime"
#     observation_definition = "n4e_hyd:observation_"
#     observation_date_time = datetime(year=2000, month=10, day=14, hour=11, minute=9, second=9)
#     sensor_ID = 1
#     all_triples = []
#     observation_no = 53_2
#
#
#     observation_mapping(given_list, observation_date_time, sensor_ID, all_triples, observation_definition,
#                         result_time_term, has_result_term, has_sim_result_term, observation_no, value_from_csv)
#
#     for triple_subset in all_triples:
#         if len(triple_subset) == 3:
#             triple_1 = triple_subset[0]
#             triple_2 = triple_subset[1]
#             triple_3 = triple_subset[2]
#             print(f"{triple_1} {triple_2} {triple_3} .")


# ## New function with possible to intergrate sensor ID to observation no

# from datetime import datetime
# def observation_mapping(given_list, observation_date_time, sensor_ID, all_triples, observation_definition,
#                         result_time_term, has_result_term, has_sim_result_term,
#                         observation_number, value_from_csv):
#     # Ensure observation_number is treated as a string
#     observation_number = str(observation_number)

#     if isinstance(given_list, list):
#         for i, sublist in enumerate(given_list):
#             # Handling result time
#             if sublist[0] == result_time_term:
#                 temp_triple = [
#                     observation_definition + observation_number, sublist[0],
#                     f'"{observation_date_time.year}-{observation_date_time.month:02d}-{observation_date_time.day:02d}'
#                     f'T{observation_date_time.hour:02d}:{observation_date_time.minute:02d}:{observation_date_time.second:02d}'
#                     f'Z"^^{sublist[-1]}'
#                 ]
#             # Handling simple result
#             elif sublist[0] == has_sim_result_term:
#                 temp_triple = [
#                     observation_definition + observation_number, sublist[0], '"' + str(value_from_csv) + '"'
#                 ]
#             # Handling simple result with nested structure
#             elif isinstance(sublist[0], list) and sublist[0][0] == has_sim_result_term and isinstance(sublist[1], list):
#                 temp_triple = [
#                     observation_definition + observation_number, sublist[0][0],
#                     '"' + str(value_from_csv) + '"' + sublist[1][0]
#                 ]
#             # Handling complex results with units
#             elif sublist[0] == has_result_term and isinstance(sublist[1], list):
#                 temp_triple = [
#                     observation_definition + observation_number, sublist[0],
#                     f"""[ 
#                     {sublist[1][0][0]}  {sublist[1][0][1]};
#                     {sublist[1][1][0]}  "{str(value_from_csv)}"{sublist[1][1][1]};
#                     {sublist[1][2][0]}  {sublist[1][2][1]}]"""
#                 ]
#             # Handling nested lists like hasMaxValue
#             elif isinstance(sublist[1], list) and isinstance(sublist[1][0], list):
#                 value_node = f"""
#                     {sublist[1][0][0]}  {sublist[1][0][1]};
#                     {sublist[1][1][0]}  "{str(value_from_csv)}"{sublist[1][1][1]};
#                     {sublist[1][2][0]}  {sublist[1][2][1]}
#                 """
#                 temp_triple = [
#                     observation_definition + observation_number, sublist[0], f"""[ 
#                     {value_node.strip()}]"""
#                 ]
#             # Handling blank nodes
#             elif sublist[0] != has_result_term and isinstance(sublist[-1], str) and sublist[-1].endswith("_"):
#                 temp_triple = [
#                     observation_definition + observation_number, sublist[0], sublist[-1] + str(sensor_ID)
#                 ]
#             # Handling other cases
#             elif isinstance(sublist[-1], str) and not sublist[-1].endswith("_"):
#                 temp_triple = [
#                     observation_definition + observation_number, sublist[0], sublist[-1]
#                 ]
#             else:
#                 temp_triple = []

#             if temp_triple:
#                 all_triples.append(temp_triple)



# if __name__ == "__main__":
#     given_list = [["sosa:observedProperty", "n4e_hyd:AirTemperature2m"],
#                   ["sosa:hasFeatureOfInterest", "n4e_hyd:catchment_"],
#                   ["sosa:madeBySensor", "n4e_hyd:sensor_"],
#                   ["sosa:memberOf", "sosa:observationCollection_"],
#                   ["sosa:resultTime", "xsd:dateTime"],
#                   ["sosa:hasResult", "n4e_hyd:temperature2mResult_"],
#                   ["rdf:type", "sosa:Result"],
#                   ["cpmeta:hasMaxValue",
#                    [["rdf:type", "qudt:QuantityValue "],
#                     ["qudt:numericValue", "^^xsd:decimal"],
#                     ["qudt:unit", "unit:DEG_C"]]]]

#     value_from_csv = 1.2334
#     has_result_term = "sosa:hasResult"
#     has_sim_result_term = "sosa:hasSimpleResult"
#     result_time_term = "sosa:resultTime"
#     observation_definition = "n4e_hyd:observation_"
#     observation_date_time = datetime(year=2000, month=10, day=14, hour=11, minute=9, second=9)
#     sensor_ID = 1
#     all_triples = []
#     observation_no = "5_32"  # Define it directly as a string with underscores

#     observation_mapping(given_list, observation_date_time, sensor_ID, all_triples, observation_definition,
#                         result_time_term, has_result_term, has_sim_result_term, observation_no, value_from_csv)

#     for triple_subset in all_triples:
#         if len(triple_subset) == 3:
#             triple_1 = triple_subset[0]
#             triple_2 = triple_subset[1]
#             triple_3 = triple_subset[2]
#             print(f"{triple_1} {triple_2} {triple_3} .")




#     # given_list = [["sosa:observedProperty", "n4e_hyd:AirTemperature2m"],
#     #               ["sosa:hasFeatureOfInterest", "n4e_hyd:catchment_"],
#     #               ["sosa:madeBySensor", "n4e_hyd:sensor_"],
#     #               ["sosa:resultTime", "xsd:dateTime"],
#     #               ["sosa:hasResult", "n4e_hyd:temperature2mResult_"],
#     #               ["rdf:type", "sosa:Result"],
#     #               ["cpmeta:hasMaxValue",
#     #                [["rdf:type", "qudt:QuantityValue "],
#     #                 ["qudt:numericValue", "^^xsd:decimal"],
#     #                 ["qudt:unit", "unit:DEG_C"]]]]


# observation_mapping_function.py

from datetime import datetime, date


def _to_xsd_datetime_literal(observation_date_time, datatype_iri="xsd:dateTime"):
    """
    Returns a Turtle-ready literal like:
    "2000-10-14T11:09:09Z"^^xsd:dateTime
    """
    # If already datetime/date
    if isinstance(observation_date_time, datetime):
        dt = observation_date_time
        return f"\"{dt:%Y-%m-%dT%H:%M:%S}Z\"^^{datatype_iri}"
    if isinstance(observation_date_time, date):
        return f"\"{observation_date_time:%Y-%m-%d}T00:00:00Z\"^^{datatype_iri}"

    # If string
    s = str(observation_date_time).strip()
    if s.endswith("Z"):
        return f"\"{s}\"^^{datatype_iri}"
    # If looks like "YYYY-MM-DD"
    if len(s) == 10 and s[4] == "-" and s[7] == "-":
        return f"\"{s}T00:00:00Z\"^^{datatype_iri}"
    # If looks like "YYYY-MM-DDTHH:MM:SS"
    if "T" in s:
        return f"\"{s}Z\"^^{datatype_iri}"
    # Fallback
    return f"\"{s}\"^^{datatype_iri}"

def observation_mapping(
    given_list,
    observation_date_time,
    sensor_ID,
    all_triples,
    observation_definition,
    result_time_term,
    has_result_term,
    has_sim_result_term,
    observation_number,
    value_from_csv,
    gauge_or_catchment="g",
):
    """
    Creates SOSA-complete observation triples.

    Always emits:
      - rdf:type sosa:Observation
      - sosa:madeBySensor hyobs:sensor_<ID>
      - sosa:hasFeatureOfInterest hyobs:<g|c>_<ID>
      - sosa:resultTime "..."^^xsd:date or xsd:dateTime

    Then applies the column-specific mapping logic
    exactly as defined in the mapping JSON.
    """

    # ---------------------------
    # BASIC URI SETUP
    # ---------------------------
    observation_number = str(observation_number)

    obs_uri = observation_definition + observation_number
    sensor_uri = f"hyobs:sensor_{sensor_ID}"
    foi_uri = f"hyobs:{gauge_or_catchment}_{sensor_ID}"

    # ---------------------------
    # REQUIRED SOSA CORE TRIPLES
    # ---------------------------
    all_triples.append([obs_uri, "rdf:type", "sosa:Observation"])
    all_triples.append([obs_uri, "sosa:madeBySensor", sensor_uri])
    all_triples.append([obs_uri, "sosa:hasFeatureOfInterest", foi_uri])

    # ---------------------------
    # RESULT TIME (SAFE LITERAL)
    # ---------------------------
    # if observation_date_time is not None:
    #     # Expecting already formatted literal OR raw string
    #     if observation_date_time.startswith('"'):
    #         time_literal = observation_date_time
    #     else:
    #         time_literal = f"\"{observation_date_time}\"^^xsd:date"
    #     all_triples.append([obs_uri, result_time_term, time_literal])
    from datetime import datetime, date

    # ---------------------------
    # RESULT TIME (ROBUST HANDLING)
    # ---------------------------
    if observation_date_time is not None:

        # Case 1: datetime.datetime
        if isinstance(observation_date_time, datetime):
            time_literal = (
                f"\"{observation_date_time.isoformat()}\"^^xsd:dateTime"
            )

        # Case 2: datetime.date
        elif isinstance(observation_date_time, date):
            time_literal = (
                f"\"{observation_date_time.isoformat()}\"^^xsd:date"
            )

        # Case 3: already a typed literal string
        elif isinstance(observation_date_time, str) and observation_date_time.startswith('"'):
            time_literal = observation_date_time

        # Case 4: raw string
        elif isinstance(observation_date_time, str):
            time_literal = f"\"{observation_date_time}\"^^xsd:date"

        else:
            # Fallback (stringify)
            time_literal = f"\"{str(observation_date_time)}\"^^xsd:date"

        all_triples.append([
            obs_uri,
            result_time_term,
            time_literal
        ])
    # ---------------------------
    # APPLY MAPPING JSON LOGIC
    # ---------------------------
    if not isinstance(given_list, list):
        return

    for sublist in given_list:

        if not isinstance(sublist, list) or len(sublist) == 0:
            continue

        pred = sublist[0]

        # Skip predicates already handled above
        if pred in (
            "rdf:type",
            "sosa:madeBySensor",
            "sosa:hasFeatureOfInterest",
            result_time_term,
        ):
            continue

        # --------------------------------------------------
        # 1) SIMPLE LITERAL RESULT
        # --------------------------------------------------
        if pred == has_sim_result_term:
            all_triples.append([
                obs_uri,
                pred,
                f"\"{value_from_csv}\""
            ])
            continue

        # --------------------------------------------------
        # 2) SIMPLE LITERAL WITH DATATYPE
        # Pattern: [[ "sosa:hasSimpleResult" ], [ "^^xsd:decimal" ]]
        # --------------------------------------------------
        if isinstance(pred, list) and pred[0] == has_sim_result_term:
            dtype = ""
            if len(sublist) > 1 and isinstance(sublist[1], list) and len(sublist[1]) > 0:
                dtype = sublist[1][0]
            all_triples.append([
                obs_uri,
                pred[0],
                f"\"{value_from_csv}\"{dtype}"
            ])
            continue

        # --------------------------------------------------
        # 3) QUDT QuantityValue BLANK NODE
        # Pattern:
        # ["sosa:hasResult", [[rdf:type,...],[qudt:numericValue,...],[qudt:unit,...]]]
        # --------------------------------------------------
        if pred == has_result_term and len(sublist) > 1 and isinstance(sublist[1], list):
            try:
                t0 = sublist[1][0]  # rdf:type
                t1 = sublist[1][1]  # qudt:numericValue
                t2 = sublist[1][2]  # qudt:unit

                obj = (
                    "[ \n"
                    f"                    {t0[0]}  {t0[1]};\n"
                    f"                    {t1[0]}  \"{value_from_csv}\"{t1[1]};\n"
                    f"                    {t2[0]}  {t2[1]}] "
                )
                all_triples.append([obs_uri, pred, obj])
            except Exception:
                all_triples.append([obs_uri, pred, f"\"{value_from_csv}\""])
            continue

        # --------------------------------------------------
        # 4) GENERIC BLANK NODE (cpmeta, stats, etc.)
        # --------------------------------------------------
        if (
            len(sublist) > 1
            and isinstance(sublist[1], list)
            and isinstance(sublist[1][0], list)
        ):
            try:
                t0 = sublist[1][0]
                t1 = sublist[1][1]
                t2 = sublist[1][2]

                obj = (
                    "[ \n"
                    f"                    {t0[0]}  {t0[1]};\n"
                    f"                    {t1[0]}  \"{value_from_csv}\"{t1[1]};\n"
                    f"                    {t2[0]}  {t2[1]}] "
                )
                all_triples.append([obs_uri, pred, obj])
            except Exception:
                pass
            continue

        # --------------------------------------------------
        # 5) URI WITH SENSOR SUFFIX
        # Pattern: "xxx_"
        # --------------------------------------------------
        if isinstance(sublist[-1], str) and sublist[-1].endswith("_"):
            all_triples.append([
                obs_uri,
                pred,
                sublist[-1] + str(sensor_ID)
            ])
            continue

        # --------------------------------------------------
        # 6) DATATYPE SUFFIX (e.g. ^^xsd:decimal)
        # --------------------------------------------------
        if isinstance(sublist[-1], str) and sublist[-1].startswith("^^"):
            all_triples.append([
                obs_uri,
                pred,
                f"\"{value_from_csv}\"{sublist[-1]}"
            ])
            continue

        # --------------------------------------------------
        # 7) NORMAL URI OR STRING
        # --------------------------------------------------
        if isinstance(sublist[-1], str):
            all_triples.append([obs_uri, pred, sublist[-1]])
            continue


if __name__ == "__main__":
    # Minimal test
    given_list = [
        ["sosa:observedProperty", "envthes:30106"],
        ["sosa:hasResult",
         [
             ["rdf:type", "qudt:QuantityValue"],
             ["qudt:numericValue", "^^xsd:decimal"],
             ["qudt:unit", "unit:MilliM"]
         ]
        ],
    ]

    all_triples = []
    observation_mapping(
        given_list=given_list,
        observation_date_time=datetime(2000, 10, 14, 11, 9, 9),
        sensor_ID="01022500",
        all_triples=all_triples,
        observation_definition="n4e_hyd:observation_",
        result_time_term="sosa:resultTime",
        has_result_term="sosa:hasResult",
        has_sim_result_term="sosa:hasSimpleResult",
        observation_number="01022500_g_1",
        value_from_csv=0.0,
        gauge_or_catchment="g",
    )

    for t in all_triples:
        if len(t) == 3:
            print(f"{t[0]} {t[1]} {t[2]} .")
