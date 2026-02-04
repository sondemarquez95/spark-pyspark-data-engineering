from pyspark.sql import SparkSession

# LOCAL (desarrollo)
spark_local = SparkSession.builder \
    .appName("Spark_Local") \
    .master("local[*]") \
    .getOrCreate()

print("Modo local:", spark_local.sparkContext.master)

# CLUSTER (ejemplo YARN)
spark_cluster = SparkSession.builder \
    .appName("Spark_Cluster") \
    .master("yarn") \
    .getOrCreate()

print("Modo cluster:", spark_cluster.sparkContext.master)
