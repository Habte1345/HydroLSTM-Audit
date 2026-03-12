import pandas as pd


def convert_csv_folder(folder_path):

    import os

    dataframes = []

    for file in os.listdir(folder_path):

        if file.endswith(".csv"):
            df = pd.read_csv(os.path.join(folder_path, file))
            dataframes.append(df)

    return dataframes


def convert_csv_with_coords(csv_file):

    df = pd.read_csv(csv_file)

    if "latitude" in df.columns and "longitude" in df.columns:
        return df

    raise ValueError("CSV must contain latitude and longitude columns")


def categorize_file(filename):

    filename = filename.lower()

    if "streamflow" in filename:
        return "streamflow"

    if "precipitation" in filename:
        return "precipitation"

    if "temperature" in filename:
        return "temperature"

    return "unknown"


def check_values_in_list(value, allowed_values):

    if value not in allowed_values:
        raise ValueError(f"{value} not allowed")

    return True


def extract_sensor_id(filename):

    import re

    match = re.search(r"\d+", filename)

    if match:
        return match.group()

    return None


def format_float_columns(df):

    for col in df.select_dtypes(include=["float", "int"]).columns:
        df[col] = df[col].astype(float)

    return df


def detect_delimiter(file_path):

    with open(file_path, "r") as f:
        first_line = f.readline()

    if ";" in first_line:
        return ";"

    if "," in first_line:
        return ","

    return ","