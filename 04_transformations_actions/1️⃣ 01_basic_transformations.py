from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("BasicTransformations") \
    .getOrCreate()

df = spark.read.parquet("data/ventas.parquet")

# Seleccionar columnas
df.select("id_venta", "monto").show()

# Filtrar registros
df.filter(col("monto") > 1000).show()

# Crear columna calculada
df = df.withColumn("monto_con_iva", col("monto") * 1.19)

# Eliminar columnas
df = df.drop("moneda")

df.show()
