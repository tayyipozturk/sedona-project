from pyspark.sql.functions import col, expr
from sedona.register import SedonaRegistrator

def compute_speed_estimation(edges_df, spark):
    SedonaRegistrator.registerAll(spark)

    df = edges_df.withColumn("length", col("length").cast("double")) \
                 .withColumn("weight_time", col("weight_time").cast("double")) \
                 .withColumn("geometry", expr("trim(geometry)")) \
                 .withColumn("geom", expr("ST_GeomFromWKT(geometry)")) \
                 .filter("geom IS NOT NULL")

    df.createOrReplaceTempView("edges")

    result_df = spark.sql("""
        SELECT
            geometry,
            length,
            weight_time,
            (length / (weight_time + 1e-6)) AS estimated_speed
        FROM edges
        WHERE length IS NOT NULL AND weight_time IS NOT NULL
    """)

    return result_df
