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

    if row_number >= len(dataframe):
        raise IndexError("Row number out of range")

    row = dataframe.iloc[row_number]

    # --------------------------------------------------
    # single date column
    # --------------------------------------------------
    if parsing_method == "single_date_column":

        date_str = str(row[date_col]).strip()

        if not date_format:
            date_format = "%Y-%m-%d"

        return datetime.strptime(date_str, date_format)

    # --------------------------------------------------
    # separate date + time
    # --------------------------------------------------
    elif parsing_method == "Separate Date and Time Columns":

        date_str = str(row[date_col]).strip()

        if time_col:
            time_str = str(row[time_col]).strip()
            combined = f"{date_str} {time_str}"
            fmt = f"{date_format} {time_format}"

        else:
            combined = date_str
            fmt = date_format

        return datetime.strptime(combined, fmt)

    # --------------------------------------------------
    # separate Y/M/D columns
    # --------------------------------------------------
    elif parsing_method == "Separate Columns for Year/Month/Day/Hour/Minute/Second":

        year = int(row[time_data_json_dictionary["year"]])
        month = int(row.get(time_data_json_dictionary.get("month", ""), 1))
        day = int(row.get(time_data_json_dictionary.get("day", ""), 1))
        hour = int(row.get(time_data_json_dictionary.get("hour", ""), 0))
        minute = int(row.get(time_data_json_dictionary.get("minute", ""), 0))
        second = int(row.get(time_data_json_dictionary.get("second", ""), 0))

        return datetime(year, month, day, hour, minute, second)

    else:
        raise ValueError("Invalid parsing method")