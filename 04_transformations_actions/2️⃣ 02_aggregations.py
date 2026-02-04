from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg

spark = SparkSession.builder \
    .appName("Aggregations") \
    .getOrCreate()

df = spark.read.parquet("data/ventas.parquet")

# Agrupar por cliente
df_grouped = df.groupBy("id_cliente") \
    .agg(
        sum("monto").alias("total_ventas"),
        avg("monto").alias("promedio_venta")
    )

df_grouped.show()
