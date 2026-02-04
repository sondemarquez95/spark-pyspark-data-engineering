from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("StopSparkSession") \
    .getOrCreate()

print("Spark activa")

# Detener sesión
spark.stop()

print("SparkSession detenida correctamente")
