from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from config.performance_util import performance_logged

@performance_logged(label="count_intersections", show=False, save_path="count_intersections")
def count_intersections(df: DataFrame, node_id_col: str = "highway") -> int:
    return df.select(node_id_col).distinct().count()

@performance_logged(label="high_degree_nodes", show=False, save_path="high_degree_nodes")
def high_degree_nodes(df: DataFrame, node_id_col: str = "highway", threshold: int = 4) -> DataFrame:
    return df.groupBy(node_id_col).count().filter(col("count") >= threshold)

@performance_logged(label="edge_density", show=False, save_path="edge_density")
def calculate_edge_density(edges_df: DataFrame, nodes_df: DataFrame) -> float:
    edge_count = edges_df.count()
    node_count = nodes_df.select("highway").distinct().count()
    if node_count == 0:
        return 0.0
    return edge_count / node_count
