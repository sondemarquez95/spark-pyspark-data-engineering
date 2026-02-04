from pyspark.sql import SparkSession

# Crear SparkSession con configuraciones típicas de ETL
spark = SparkSession.builder \
    .appName("ETL_Spark_Session") \
    .master("local[*]") \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", "2") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

print("Spark Session creada correctamente")
print("Executor memory:", spark.conf.get("spark.executor.memory"))
print("Shuffle partitions:", spark.conf.get("spark.sql.shuffle.partitions"))
