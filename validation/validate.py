import csv
from shapely import wkt
from pyspark.sql import SparkSession

def validate_wkt_geometry(csv_path: str, geometry_col: str = "geometry", output_clean_path: str = "valid_nodes.csv"):
    sedona = SparkSession.builder.appName("Geometry Validator").getOrCreate()
    df = sedona.read.option("header", True).csv(csv_path)

    # Collect and validate WKT rows in Python
    rows = df.collect()
    valid_rows = []
    invalid_rows = []

    for row in rows:
        try:
            geometry_str = row[geometry_col]
            if geometry_str is None:
                raise ValueError("Empty geometry")
            wkt.loads(geometry_str.strip())  # will raise if invalid
            valid_rows.append(row)
        except Exception:
            invalid_rows.append(row)

    # Print invalid geometries
    print(f"[VALIDATOR] Found {len(invalid_rows)} invalid geometries:")
    for bad_row in invalid_rows:
        print(bad_row.asDict())

    # Save clean data to a new CSV
    if valid_rows:
        valid_df = sedona.createDataFrame(valid_rows, schema=df.schema)
        valid_df.write.mode("overwrite").option("header", True).csv(output_clean_path)
        print(f"[VALIDATOR] Clean data written to: {output_clean_path}")
        return valid_df
    else:
        print("[VALIDATOR] No valid geometries found.")
        return sedona.createDataFrame([], schema=df.schema)

# Example usage:
# validate_wkt_geometry("data/nodes_ankara.csv", "geometry", "data/nodes_ankara_clean.csv")

validate_wkt_geometry("data/nodes_ankara.csv", "geometry", "data/nodes_ankara_clean.csv")