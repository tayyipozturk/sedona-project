# src/analysis/speed_profile.py
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, avg, count
from config.performance_util import performance_logged

@performance_logged(label="average_speed_by_road_type", show=False, save_path="average_speed_by_road_type.csv")
def average_speed_by_road_type(edges_df: DataFrame) -> DataFrame:
    return edges_df.groupBy("highway") \
                   .agg(avg(col("maxspeed")).alias("avg_maxspeed"),
                        count("highway").alias("segment_count")) \
                   .orderBy(col("segment_count").desc())

@performance_logged(label="average_travel_time", show=False, save_path="average_travel_time.csv")
def average_travel_time(edges_df: DataFrame) -> float:
    return edges_df.select(avg(col("weight_time"))).first()[0]

@performance_logged(label="fastest_road_segments", show=False, save_path="fastest_road_segments.csv")
def fastest_road_segments(edges_df: DataFrame, top_n: int = 10) -> DataFrame:
    return edges_df.select("osmid", "highway", "maxspeed", "weight_time") \
                   .orderBy(col("maxspeed").desc_nulls_last()) \
                   .limit(top_n)