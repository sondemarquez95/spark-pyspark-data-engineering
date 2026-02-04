from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Joins") \
    .getOrCreate()

ventas = spark.read.parquet("data/ventas.parquet")
clientes = spark.read.parquet("data/clientes.parquet")

# INNER JOIN
df_inner = ventas.join(
    clientes,
    ventas.id_cliente == clientes.id_cliente,
    "inner"
)

# LEFT JOIN
df_left = ventas.join(
    clientes,
    "id_cliente",
    "left"
)

df_inner.show()
df_left.show()
