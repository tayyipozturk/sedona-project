from pyspark.sql.functions import col, expr

def estimate_betweenness_proxy(nodes_df, edges_df, sedona, distance=0.001):
    n = nodes_df.withColumn("x", col("x").cast("double")) \
                .withColumn("y", col("y").cast("double")) \
                .withColumn("geometry", expr("trim(geometry)")) \
                .withColumn("geom", expr("ST_GeomFromWKT(geometry)"))

    e = edges_df.withColumn("geometry", expr("trim(geometry)")) \
                .withColumn("geom", expr("ST_GeomFromWKT(geometry)"))

    n.createOrReplaceTempView("nodes")
    e.createOrReplaceTempView("edges")

    return sedona.sql(f"""
        SELECT
            n.x,
            n.y,
            COUNT(e.geometry) AS intersecting_paths
        FROM nodes n, edges e
        WHERE ST_DWithin(n.geom, e.geom, {distance})
        GROUP BY n.x, n.y
        ORDER BY intersecting_paths DESC
    """)

def detect_cycles(edges_df, sedona, tolerance=0.0005):
    df = edges_df.withColumn("geometry", expr("trim(geometry)")) \
                 .withColumn("geom", expr("ST_GeomFromWKT(geometry)"))

    df.createOrReplaceTempView("edges")

    return sedona.sql(f"""
        SELECT a.geometry AS edge1, b.geometry AS edge2, c.geometry AS edge3
        FROM edges a, edges b, edges c
        WHERE ST_DWithin(ST_EndPoint(a.geom), ST_StartPoint(b.geom), {tolerance})
          AND ST_DWithin(ST_EndPoint(b.geom), ST_StartPoint(c.geom), {tolerance})
          AND ST_DWithin(ST_EndPoint(c.geom), ST_StartPoint(a.geom), {tolerance})
          AND a.geometry != b.geometry AND b.geometry != c.geometry AND a.geometry != c.geometry
        LIMIT 100
    """)

def detect_subnetworks_by_attribute(edges_df, sedona, attribute="highway"):
    df = edges_df.withColumn(attribute, col(attribute)) \
                 .withColumn("geometry", expr("trim(geometry)")) \
                 .withColumn("geom", expr("ST_GeomFromWKT(geometry)")) \
                 .filter(f"{attribute} IS NOT NULL")

    df.createOrReplaceTempView("edges")

    return sedona.sql(f"""
        SELECT {attribute}, COUNT(*) AS segment_count
        FROM edges
        GROUP BY {attribute}
        ORDER BY segment_count DESC
    """)

def check_directional_conflicts(edges_df, sedona):
    df = edges_df.withColumn("geometry", expr("trim(geometry)")) \
                 .withColumn("geom", expr("ST_GeomFromWKT(geometry)")) \
                 .filter("oneway IS NOT NULL")

    df.createOrReplaceTempView("edges")

    return sedona.sql("""
        SELECT a.geometry AS road_a, b.geometry AS road_b, a.oneway AS dir_a, b.oneway AS dir_b
        FROM edges a, edges b
        WHERE ST_DWithin(a.geom, b.geom, 0.0005)
          AND a.geometry != b.geometry
          AND a.oneway != b.oneway
    """)

def classify_junction_shapes(nodes_df, edges_df, sedona):
    n = nodes_df.withColumn("x", col("x").cast("double")) \
                .withColumn("y", col("y").cast("double")) \
                .withColumn("geometry", expr("trim(geometry)")) \
                .withColumn("geom", expr("ST_GeomFromWKT(geometry)"))

    e = edges_df.withColumn("geometry", expr("trim(geometry)")) \
                .withColumn("geom", expr("ST_GeomFromWKT(geometry)"))

    n.createOrReplaceTempView("nodes")
    e.createOrReplaceTempView("edges")

    return sedona.sql("""
        SELECT
            n.x,
            n.y,
            COUNT(e.geometry) AS touching_roads,
            CASE
                WHEN COUNT(e.geometry) = 1 THEN 'dead-end'
                WHEN COUNT(e.geometry) = 2 THEN 'T/Y'
                WHEN COUNT(e.geometry) = 3 THEN '3-way'
                WHEN COUNT(e.geometry) = 4 THEN '4-way'
                ELSE 'complex'
            END AS junction_type
        FROM nodes n, edges e
        WHERE ST_DWithin(n.geom, e.geom, 0.0005)
        GROUP BY n.x, n.y
    """)