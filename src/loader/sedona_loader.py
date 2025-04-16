from sedona.register import SedonaRegistrator
from sedona.core.SpatialRDD import PointRDD, LineStringRDD
from sedona.utils.adapter import Adapter
from pyspark.sql.functions import col, expr, trim

def load_csv_as_dataframe(sedona, path: str):
    SedonaRegistrator.registerAll(sedona)
    return sedona.read.option("header", True).csv(path)


def add_geometry_column(df, geometry_column_name="geometry"):
    return df.withColumn(geometry_column_name, expr(f"ST_GeomFromWKT({geometry_column_name})"))


def create_point_rdd(df, geometry_column="geometry") -> PointRDD:
    df = df.withColumn(geometry_column, col(geometry_column).cast("string"))
    df = df.withColumn(geometry_column, trim(col(geometry_column)))
    
    # Create new Geometry column
    df = df.withColumn("geom", expr(f"ST_GeomFromWKT({geometry_column})"))
    
    # Filter invalid geometries
    df = df.filter("geom IS NOT NULL")

    # Convert to PointRDD
    point_rdd = Adapter.toSpatialRdd(df, "geom")
    point_rdd.analyze()

    return point_rdd

def create_linestring_rdd(df, geometry_column="geometry") -> LineStringRDD:
    line_rdd = Adapter.toSpatialRdd(df, geometry_column)
    line_rdd.analyze()
    return line_rdd


def load_nodes_as_point_rdd(sedona, path):
    df = load_csv_as_dataframe(sedona, path)
    df = add_geometry_column(df, "geometry")
    return create_point_rdd(df)


def load_edges_as_linestring_rdd(sedona, path):
    df = load_csv_as_dataframe(sedona, path)
    df = add_geometry_column(df, "geometry")
    return create_linestring_rdd(df)
