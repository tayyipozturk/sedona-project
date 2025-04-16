# src/analysis/connectivity.py
from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def count_intersections(df: DataFrame, node_id_col: str = "highway") -> int:
    return df.select(node_id_col).distinct().count()


def high_degree_nodes(df: DataFrame, node_id_col: str = "highway", threshold: int = 4) -> DataFrame:
    return df.groupBy(node_id_col).count().filter(col("count") >= threshold)


def calculate_edge_density(edges_df: DataFrame, nodes_df: DataFrame) -> float:
    edge_count = edges_df.count()
    node_count = nodes_df.select("highway").distinct().count()
    if node_count == 0:
        return 0.0
    return edge_count / node_count
