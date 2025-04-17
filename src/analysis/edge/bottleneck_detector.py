from pyspark.sql.functions import col, expr
from config.performance_util import performance_logged

@performance_logged(label="compute_bottleneck_score", show=False, save_path="bottleneck_score.csv")
def compute_bottleneck_score(edges_df, sedona):
    df = edges_df.withColumn("length", col("length").cast("double")) \
                 .withColumn("width", col("width").cast("double")) \
                 .withColumn("weight_time", col("weight_time").cast("double")) \
                 .withColumn("geometry", expr("trim(geometry)")) \
                 .withColumn("geom", expr("ST_GeomFromWKT(geometry)")) \
                 .filter("geom IS NOT NULL")

    df.createOrReplaceTempView("edges")

    result_df = sedona.sql("""
        SELECT
            geometry,
            length,
            width,
            weight_time,
            (length * weight_time) / (width + 0.1) AS bottleneck_score
        FROM edges
        WHERE length IS NOT NULL AND weight_time IS NOT NULL AND width IS NOT NULL
    """)

    return result_df
