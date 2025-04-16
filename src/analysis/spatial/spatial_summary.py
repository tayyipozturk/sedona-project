from sedona.core.SpatialRDD import SpatialRDD

def summarize_spatial_rdd(rdd: SpatialRDD, label: str = "SpatialRDD") -> dict:
    return {
        "label": label,
        "count": rdd.rawSpatialRDD.count(),
        "envelope": str(rdd.boundaryEnvelope),
        "spatialPartitioning": str(rdd.getPartitioner()),
        "approximateTotalCount": rdd.approximateTotalCount,
    }