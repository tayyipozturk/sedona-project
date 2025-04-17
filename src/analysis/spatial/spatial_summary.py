from sedona.core.SpatialRDD import SpatialRDD
from config.performance_util import performance_logged

@performance_logged(label="summarize_spatial_rdd", show=False, save_path="summarize_spatial_rdd")
def summarize_spatial_rdd(rdd: SpatialRDD, label: str = "SpatialRDD") -> dict:
    return {
        "label": label,
        "count": rdd.rawSpatialRDD.count(),
        "envelope": str(rdd.boundaryEnvelope),
        "spatialPartitioning": str(rdd.getPartitioner()),
        "approximateTotalCount": rdd.approximateTotalCount,
    }