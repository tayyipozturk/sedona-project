from analysis.spatial.spatial_ops import spatial_join_points_with_polygon
from sedona.utils.adapter import Adapter
from sedona.register import SedonaRegistrator
from pyspark.sql.functions import expr
from src.loader.sedona_loader import load_csv_as_dataframe, add_geometry_column



def test_spatial_ops(spark, points_rdd):
    print("[TEST] Loading point data for spatial RDD test...")
    print("[TEST] Creating test polygon...")
    polygon_wkt = "POLYGON((32.5 39.6, 32.5 40.1, 33.0 40.1, 33.0 39.6, 32.5 39.6))"
    polygon_df = spark.createDataFrame([(polygon_wkt,)], ["geometry"])
    polygon_df = polygon_df.withColumn("geometry", expr("ST_GeomFromWKT(geometry)"))
    polygon_rdd = Adapter.toSpatialRdd(polygon_df, "geometry")
    polygon_rdd.analyze()

    print("[TEST] Running safe spatial join without indexedRawRDD...")
    joined = spatial_join_points_with_polygon(points_rdd, polygon_rdd)
    print(f"[TEST] Spatial Join Result Count: {joined.count()}")

    print("[TEST] Loading and buffering point data using SQL...")
    SedonaRegistrator.registerAll(spark)

    df = load_csv_as_dataframe(spark, "data/nodes_ankara.csv")
    df = add_geometry_column(df)
    df = df.withColumn("geometry", expr("ST_Buffer(geometry, 0.01)"))
    buffered_rdd = Adapter.toSpatialRdd(df, "geometry")
    buffered_rdd.analyze()

    print(f"[TEST] Buffered Points Count: {buffered_rdd.rawSpatialRDD.count()}")
    print("\n")