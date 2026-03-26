from pyproj import Transformer


def transform_coordinates(dataframe, x_col, y_col, input_epsg="EPSG:3035", output_epsg="EPSG:4326"):

    transformer = Transformer.from_crs(input_epsg, output_epsg, always_xy=True)

    dataframe[[x_col, y_col]] = dataframe.apply(
        lambda row: transformer.transform(row[x_col], row[y_col]),
        axis=1,
        result_type="expand",
    )

    return dataframe


def ensure_wgs84(gdf):

    if gdf.crs is None:
        raise ValueError("Dataset CRS undefined")

    if gdf.crs.to_string() != "EPSG:4326":
        gdf = gdf.to_crs("EPSG:4326")

    return gdf