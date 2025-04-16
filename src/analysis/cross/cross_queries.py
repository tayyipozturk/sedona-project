from pyspark.sql.functions import col, expr

def find_edges_near_nodes(edges_df, nodes_df, sedona, distance=0.001):
    e = edges_df.withColumn("geometry", expr("trim(geometry)")) \
                .withColumn("geom", expr("ST_GeomFromWKT(geometry)")) \
                .filter("geom IS NOT NULL")
    n = nodes_df.withColumn("geometry", expr("trim(geometry)")) \
                .withColumn("geom", expr("ST_GeomFromWKT(geometry)")) \
                .filter("geom IS NOT NULL")

    e.createOrReplaceTempView("edges")
    n.createOrReplaceTempView("nodes")

    result = sedona.sql(f"""
        SELECT e.*, n.x, n.y
        FROM edges e, nodes n
        WHERE ST_Distance(e.geom, n.geom) < {distance}
    """)

    return result


def anchor_edges_to_nodes(edges_df, nodes_df, sedona, tolerance=0.0005):
    e = edges_df.withColumn("geometry", expr("trim(geometry)")) \
                .withColumn("geom", expr("ST_GeomFromWKT(geometry)")) \
                .filter("geom IS NOT NULL")
    n = nodes_df.withColumn("geometry", expr("trim(geometry)")) \
                .withColumn("geom", expr("ST_GeomFromWKT(geometry)")) \
                .filter("geom IS NOT NULL")

    e.createOrReplaceTempView("edges")
    n.createOrReplaceTempView("nodes")

    return sedona.sql(f"""
        SELECT e.*, n.x AS node_x, n.y AS node_y
        FROM edges e, nodes n
        WHERE ST_Distance(ST_StartPoint(e.geom), n.geom) < {tolerance}
           OR ST_Distance(ST_EndPoint(e.geom), n.geom) < {tolerance}
    """)


def estimate_node_road_centrality(edges_df, nodes_df, sedona):
    e = edges_df.withColumn("length", col("length").cast("double")) \
                .withColumn("geometry", expr("trim(geometry)")) \
                .withColumn("geom", expr("ST_GeomFromWKT(geometry)")) \
                .filter("geom IS NOT NULL")
    n = nodes_df.withColumn("x", col("x").cast("double")) \
                .withColumn("y", col("y").cast("double")) \
                .withColumn("geometry", expr("trim(geometry)")) \
                .withColumn("geom", expr("ST_GeomFromWKT(geometry)")) \
                .filter("geom IS NOT NULL")

    e.createOrReplaceTempView("edges")
    n.createOrReplaceTempView("nodes")

    return sedona.sql("""
        SELECT
            n.x,
            n.y,
            COUNT(e.geometry) AS connected_roads,
            SUM(e.length) AS total_road_length
        FROM edges e, nodes n
        WHERE ST_DWithin(e.geom, n.geom, 0.0005)
        GROUP BY n.x, n.y
    """)


def local_average_speed_per_node(edges_df, nodes_df, sedona):
    e = edges_df.withColumn("length", col("length").cast("double")) \
                .withColumn("weight_time", col("weight_time").cast("double")) \
                .withColumn("geometry", expr("trim(geometry)")) \
                .withColumn("geom", expr("ST_GeomFromWKT(geometry)")) \
                .filter("geom IS NOT NULL")
    n = nodes_df.withColumn("x", col("x").cast("double")) \
                .withColumn("y", col("y").cast("double")) \
                .withColumn("geometry", expr("trim(geometry)")) \
                .withColumn("geom", expr("ST_GeomFromWKT(geometry)")) \
                .filter("geom IS NOT NULL")

    e.createOrReplaceTempView("edges")
    n.createOrReplaceTempView("nodes")

    return sedona.sql("""
        SELECT
            n.x,
            n.y,
            AVG(e.length / (e.weight_time + 1e-6)) AS avg_speed
        FROM edges e, nodes n
        WHERE ST_DWithin(e.geom, n.geom, 0.0005)
        GROUP BY n.x, n.y
    """)


def classify_intersection_types(nodes_df, edges_df, sedona, min_degree=3):
    n = nodes_df.withColumn("x", col("x").cast("double")) \
                .withColumn("y", col("y").cast("double")) \
                .withColumn("street_count", col("street_count").cast("int")) \
                .withColumn("geometry", expr("trim(geometry)")) \
                .withColumn("geom", expr("ST_GeomFromWKT(geometry)")) \
                .filter("street_count >= {min_degree}")
    e = edges_df.withColumn("geometry", expr("trim(geometry)")) \
                .withColumn("geom", expr("ST_GeomFromWKT(geometry)")) \
                .filter("geom IS NOT NULL")

    n.createOrReplaceTempView("nodes")
    e.createOrReplaceTempView("edges")

    return sedona.sql("""
        SELECT
            n.x,
            n.y,
            n.street_count,
            COUNT(e.geometry) AS touching_roads
        FROM nodes n, edges e
        WHERE ST_DWithin(n.geom, e.geom, 0.0005)
        GROUP BY n.x, n.y, n.street_count
        HAVING COUNT(e.geometry) >= {min_degree}
    """)


def assign_traffic_proxy_to_edges(edges_df, nodes_df, sedona):
    e = edges_df.withColumn("length", col("length").cast("double")) \
                .withColumn("geometry", expr("trim(geometry)")) \
                .withColumn("geom", expr("ST_GeomFromWKT(geometry)"))
    n = nodes_df.withColumn("street_count", col("street_count").cast("double")) \
                .withColumn("x", col("x").cast("double")) \
                .withColumn("y", col("y").cast("double")) \
                .withColumn("geometry", expr("trim(geometry)")) \
                .withColumn("geom", expr("ST_GeomFromWKT(geometry)"))

    e.createOrReplaceTempView("edges")
    n.createOrReplaceTempView("nodes")

    return sedona.sql("""
        SELECT
            e.geometry,
            e.length,
            AVG(n.street_count) AS avg_connected_street_count
        FROM edges e, nodes n
        WHERE ST_DWithin(ST_StartPoint(e.geom), n.geom, 0.0005)
           OR ST_DWithin(ST_EndPoint(e.geom), n.geom, 0.0005)
        GROUP BY e.geometry, e.length
    """)


def compute_road_class_accessibility(nodes_df, edges_df, sedona, radius=0.001):
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
            e.highway,
            COUNT(*) AS nearby_roads
        FROM nodes n, edges e
        WHERE ST_Distance(n.geom, e.geom) < {radius}
        GROUP BY n.x, n.y, e.highway
        ORDER BY n.x, n.y, nearby_roads DESC
    """)


def estimate_node_to_node_travel(edges_df, nodes_df, sedona, max_distance=0.002):
    n = nodes_df.withColumn("x", col("x").cast("double")) \
                .withColumn("y", col("y").cast("double")) \
                .withColumn("geometry", expr("trim(geometry)")) \
                .withColumn("geom", expr("ST_GeomFromWKT(geometry)"))
    e = edges_df.withColumn("length", col("length").cast("double")) \
                .withColumn("weight_time", col("weight_time").cast("double")) \
                .withColumn("geometry", expr("trim(geometry)")) \
                .withColumn("geom", expr("ST_GeomFromWKT(geometry)"))

    n.createOrReplaceTempView("nodes")
    e.createOrReplaceTempView("edges")

    return sedona.sql(f"""
        SELECT
            n1.x AS node1_x,
            n1.y AS node1_y,
            n2.x AS node2_x,
            n2.y AS node2_y,
            MIN(e.length) AS shortest_distance,
            MIN(e.weight_time) AS estimated_time
        FROM nodes n1, nodes n2, edges e
        WHERE ST_Distance(n1.geom, n2.geom) < {max_distance}
          AND ST_DWithin(e.geom, n1.geom, 0.0005)
          AND ST_DWithin(e.geom, n2.geom, 0.0005)
          AND n1.x != n2.x AND n1.y != n2.y
        GROUP BY n1.x, n1.y, n2.x, n2.y
    """)


def detect_entry_exit_nodes(nodes_df, edges_df, sedona, min_x, min_y, max_x, max_y):
    n = nodes_df.withColumn("x", col("x").cast("double")) \
                .withColumn("y", col("y").cast("double")) \
                .withColumn("geometry", expr("trim(geometry)")) \
                .withColumn("geom", expr("ST_GeomFromWKT(geometry)"))
    e = edges_df.withColumn("geometry", expr("trim(geometry)")) \
                .withColumn("geom", expr("ST_GeomFromWKT(geometry)"))

    n.createOrReplaceTempView("nodes")
    e.createOrReplaceTempView("edges")

    return sedona.sql(f"""
        SELECT DISTINCT n.*
        FROM nodes n, edges e
        WHERE ST_Intersects(n.geom, e.geom)
          AND NOT ST_Within(n.geom, ST_PolygonFromEnvelope({min_x}, {min_y}, {max_x}, {max_y}))
    """)