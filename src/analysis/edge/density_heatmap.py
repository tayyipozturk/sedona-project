from pyspark.sql.functions import col, expr

def compute_density_heatmap(edges_df, sedona, cell_size=0.01):
    df = edges_df.withColumn("length", col("length").cast("double")) \
                 .withColumn("geometry", expr("trim(geometry)")) \
                 .withColumn("geom", expr("ST_GeomFromWKT(geometry)")) \
                 .withColumn("x", expr("ST_X(ST_StartPoint(geom))")) \
                 .withColumn("y", expr("ST_Y(ST_StartPoint(geom))")) \
                 .filter("geom IS NOT NULL")

    df.createOrReplaceTempView("edges")

    result_df = sedona.sql(f"""
        SELECT
            CAST(x / {cell_size} AS INT) AS cell_x,
            CAST(y / {cell_size} AS INT) AS cell_y,
            SUM(length) AS total_road_length
        FROM edges
        WHERE length IS NOT NULL
        GROUP BY CAST(x / {cell_size} AS INT), CAST(y / {cell_size} AS INT)
    """)

    return result_df
