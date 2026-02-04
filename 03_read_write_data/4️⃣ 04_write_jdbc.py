from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("WriteJDBC") \
    .getOrCreate()

df = spark.read.parquet("data/ventas.parquet")

jdbc_url = "jdbc:postgresql://localhost:5432/dwh"

properties = {
    "user": "user",
    "password": "password",
    "driver": "org.postgresql.Driver"
}

df.write.jdbc(
    url=jdbc_url,
    table="ventas_dw",
    mode="append",
    properties=properties
)

print("Datos cargados en la BD destino")
