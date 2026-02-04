from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("WriteFiles") \
    .getOrCreate()

df = spark.read.parquet("data/ventas.parquet")

# =====================
# Escribir CSV
# =====================
df.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("output/ventas_csv")

# =====================
# Escribir Parquet (BEST PRACTICE)
# =====================
df.write \
    .mode("overwrite") \
    .partitionBy("anio") \
    .parquet("output/ventas_parquet")
