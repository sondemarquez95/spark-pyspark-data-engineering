from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("SparkSQL") \
    .getOrCreate()

# Leer datos
df = spark.read.parquet("data/ventas.parquet")

# Crear vista temporal
df.createOrReplaceTempView("ventas")

print("Vista temporal 'ventas' creada")
