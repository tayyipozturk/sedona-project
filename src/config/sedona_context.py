
from sedona.register import SedonaRegistrator
from pyspark.sql import SparkSession

def create_sedona_session():
    sedona = (
        SparkSession.builder
        .appName("Sedona OSM Analysis")
        .config("spark.jars.packages",
            "org.apache.sedona:sedona-spark-3.3_2.12:1.7.1,"
            "org.datasyslab:geotools-wrapper:1.7.1-28.5")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryo.registrator", "org.apache.sedona.core.serde.SedonaKryoRegistrator")
        .getOrCreate()
    )

    SedonaRegistrator.registerAll(sedona)
    return sedona
