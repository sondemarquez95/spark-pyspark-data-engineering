from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ReadJDBC") \
    .getOrCreate()

jdbc_url = "jdbc:postgresql://localhost:5432/ventas_db"

properties = {
    "user": "user",
    "password": "password",
    "driver": "org.postgresql.Driver"
}

df_db = spark.read.jdbc(
    url=jdbc_url,
    table="ventas",
    properties=properties
)

df_db.show()
