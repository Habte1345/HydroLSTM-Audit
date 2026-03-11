# import pandas as pd
# import json
# from datetime import datetime
# import streamlit as st
# from io import StringIO
#
#
# def parse_csv_row(csv_content, time_data_json_dictionary, row_number, parsing_method, date_col=None, time_col=None,
#                   date_format=None, time_format=None):
#     # Load the CSV file into a DataFrame
#     csv_file_data_frame = pd.read_csv(StringIO(csv_content), delimiter=';')
#
#     # Check if the row_number is within the DataFrame's index range
#     if row_number >= len(csv_file_data_frame):
#         raise IndexError("Row number out of range")
#
#     # Select the specific row as a Series
#     row = csv_file_data_frame.iloc[row_number]
#
#     if parsing_method == "Separate Columns for Year/Month/Day/Hour/Minute/Second":
#         # Initialize time components to default values
#         year = month = day = hour = minute = second = 0
#         year = month = day = 1
#
#         # Iterate over the keys to match the time JSON and CSV heading
#         for time_key_json, time_value_json in time_data_json_dictionary.items():
#             # Check if the column exists in the DataFrame
#             if time_value_json in row:
#                 if time_key_json == 'year':
#                     year = int(row[time_value_json])
#                 elif time_key_json == 'month':
#                     month = max(1, int(row[time_value_json]))
#                 elif time_key_json == 'day':
#                     day = max(1, int(row[time_value_json]))
#                 elif time_key_json == 'hour':
#                     hour = int(row[time_value_json])
#                 elif time_key_json == 'minute':
#                     minute = int(row[time_value_json])
#                 elif time_key_json == 'second':
#                     second = int(row[time_value_json])
#
#         # Create and return datetime object
#         observation_time = datetime(year, month, day, hour, minute, second)
#         return observation_time
#
#     elif parsing_method == "Separate Date and Time Columns":
#         if date_col is None or time_col is None or date_format is None or time_format is None:
#             raise ValueError(
#                 "For 'Separate Date and Time Columns' method, date_col, time_col, date_format, and time_format must be provided.")
#
#         try:
#             date_str = csv_file_data_frame.at[row_number, date_col]
#             time_str = csv_file_data_frame.at[row_number, time_col]
#             combined_str = f"{date_str} {time_str}"
#             combined_format = f"{date_format} {time_format}"
#             observation_time = datetime.strptime(combined_str, combined_format)
#             return observation_time
#         except ValueError as e:
#             st.error(f"Error parsing date/time in row {row_number}: {e}")
#             return None
#     else:
#         raise ValueError("Invalid parsing method specified")
#
#
# # example
# if __name__ == '__main__':
#
#     csv_file_path = 'time_date_sep_test.csv'
#     time_json_path = 'mapping_time.json'
#
#     with open(time_json_path, 'r') as time_json_file:
#         time_data_json = json.load(time_json_file)
#
#
#     observation_time = parse_csv_row(csv_file_path, time_data_json, 1)
#
#     print(type(observation_time))
#
#     print("Full Date and Time:", observation_time)
#     print("Date:", observation_time.date())
#     print("Time:", observation_time.time())

## commented on 21/10/2024


# import pandas as pd
# import json
# from datetime import datetime
# import streamlit as st
# from io import StringIO


# def parse_csv_row(csv_file_data_frame, time_data_json_dictionary, row_number, parsing_method, delimiter, date_col=None, time_col=None,
#                   date_format=None, time_format=None):
#     # Load the CSV file into a DataFrame
#     #csv_file_data_frame = pd.read_csv(StringIO(csv_content), delimiter=delimiter)

#     # Check if the row_number is within the DataFrame's index range
#     if row_number >= len(csv_file_data_frame):
#         raise IndexError("Row number out of range")

#     # Select the specific row as a Series
#     row = csv_file_data_frame.iloc[row_number]

#     if parsing_method == "Separate Columns for Year/Month/Day/Hour/Minute/Second":
#         # Initialize time components to default values
#         year = month = day = hour = minute = second = 0
#         year = month = day = 1

#         # Iterate over the keys to match the time JSON and CSV heading
#         for time_key_json, time_value_json in time_data_json_dictionary.items():
#             # Check if the column exists in the DataFrame
#             if time_value_json in row:
#                 if time_key_json == 'year':
#                     year = int(row[time_value_json])
#                 elif time_key_json == 'month':
#                     month = max(1, int(row[time_value_json]))
#                 elif time_key_json == 'day':
#                     day = max(1, int(row[time_value_json]))
#                 elif time_key_json == 'hour':
#                     hour = int(row[time_value_json])
#                 elif time_key_json == 'minute':
#                     minute = int(row[time_value_json])
#                 elif time_key_json == 'second':
#                     second = int(row[time_value_json])

#         # Create and return datetime object
#         observation_time = datetime(year, month, day, hour, minute, second)
#         return observation_time

#     elif parsing_method == "Separate Date and Time Columns":
#         if date_col is None or date_format is None:
#             raise ValueError(
#                 "For 'Separate Date and Time Columns' method, date_col and date_format must be provided.")

#         try:
#             date_str = csv_file_data_frame.at[row_number, date_col]
#             if time_col is None:
#                 time_str = "00:00:00"
#                 time_format = "%H:%M:%S"
#             else:
#                 time_str = csv_file_data_frame.at[row_number, time_col]
#             combined_str = f"{date_str} {time_str}"
#             combined_format = f"{date_format} {time_format}"
#             observation_time = datetime.strptime(combined_str, combined_format)
#             return observation_time
#         except ValueError as e:
#             print(f"Error parsing date/time in row {row_number}: {e}")
#             st.error(f"Error parsing date/time in row {row_number}: {e}")
#             return None
#     else:
#         raise ValueError("Invalid parsing method specified")


# # example
# if __name__ == '__main__':

#     # csv_file_content = """date_col;time_col;other_data
#     # 2023-07-08;12:34:56;sample1
#     # 2023-07-09;;sample2
#     # """

#     csv_file_content = """YYYY;MM;DD;qobs;ckhs;qceq;qcol
# 1981;1;1;23.229;1;0;0
# 1981;1;2;30.204;1;0;0"""

#     time_json_content = """
#     {
#         "year": "YYYY",
#         "month": "MM",
#         "day": "DD",
#         "hour": "hour_col",
#         "minute": "minute_col",
#         "second": "second_col"
#     }
#     """

#     # Load JSON data
#     time_data_json = json.loads(time_json_content)

#     # Parse row 1
#     observation_time = parse_csv_row(csv_file_content, time_data_json, 1, "Separate Columns for Year/Month/Day/Hour/Minute/Second", date_col="date_col", date_format="%Y-%m-%d")

#     if observation_time:
#         print(type(observation_time))
#         print("Full Date and Time:", observation_time)
#         print("Date:", observation_time.date())
#         print("Time:", observation_time.time())
#     else:
#         print("Failed to parse the date and time.")

from datetime import datetime


def parse_csv_row(
    dataframe,
    time_data_json_dictionary,
    row_number,
    parsing_method,
    date_col=None,
    time_col=None,
    date_format=None,
    time_format=None,
):
    """
    Parse time information from a CSV row and return a datetime object.

    SUPPORTED MODES:
    1) single_date_column      (CAMELS – REQUIRED)
    2) Separate Date and Time Columns
    3) Separate Columns for Year/Month/Day/Hour/Minute/Second
    """

    # ------------------------------------------------------------
    # SAFETY CHECK
    # ------------------------------------------------------------
    if row_number >= len(dataframe):
        raise IndexError("Row number out of range")

    row = dataframe.iloc[row_number]

    # ============================================================
    # MODE 1 — SINGLE DATE COLUMN (CAMELS)
    # ============================================================
    if parsing_method == "single_date_column":
        if date_col is None:
            raise ValueError("date_col must be provided for single_date_column")

        date_str = str(row[date_col]).strip()

        if not date_format:
            date_format = "%Y-%m-%d"

        try:
            return datetime.strptime(date_str, date_format)
        except Exception as e:
            raise ValueError(
                f"Failed to parse date '{date_str}' with format '{date_format}'"
            ) from e

    # ============================================================
    # MODE 2 — SEPARATE DATE & TIME COLUMNS
    # ============================================================
    elif parsing_method == "Separate Date and Time Columns":
        if date_col is None or date_format is None:
            raise ValueError(
                "date_col and date_format are required for Separate Date and Time Columns"
            )

        date_str = str(row[date_col]).strip()

        if time_col and time_format:
            time_str = str(row[time_col]).strip()
            combined_str = f"{date_str} {time_str}"
            combined_format = f"{date_format} {time_format}"
        else:
            combined_str = date_str
            combined_format = date_format

        try:
            return datetime.strptime(combined_str, combined_format)
        except Exception as e:
            raise ValueError(
                f"Failed to parse datetime '{combined_str}' with format '{combined_format}'"
            ) from e

    # ============================================================
    # MODE 3 — SEPARATE Y / M / D / H / M / S COLUMNS
    # ============================================================
    elif parsing_method == "Separate Columns for Year/Month/Day/Hour/Minute/Second":
        if not time_data_json_dictionary:
            raise ValueError("time_data_json_dictionary is required for this mode")

        try:
            year = int(row[time_data_json_dictionary["year"]])
            month = int(row.get(time_data_json_dictionary.get("month", ""), 1))
            day = int(row.get(time_data_json_dictionary.get("day", ""), 1))
            hour = int(row.get(time_data_json_dictionary.get("hour", ""), 0))
            minute = int(row.get(time_data_json_dictionary.get("minute", ""), 0))
            second = int(row.get(time_data_json_dictionary.get("second", ""), 0))
        except KeyError as e:
            raise KeyError(
                f"Missing expected time column: {e}"
            ) from e

        return datetime(
            max(1, year),
            max(1, month),
            max(1, day),
            hour,
            minute,
            second,
        )

    # ============================================================
    # INVALID MODE
    # ============================================================
    else:
        raise ValueError(
            f"Invalid parsing method specified: {parsing_method}"
        )
