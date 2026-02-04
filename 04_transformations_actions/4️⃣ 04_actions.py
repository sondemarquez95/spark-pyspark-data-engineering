from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Actions") \
    .getOrCreate()

df = spark.read.parquet("data/ventas.parquet")

# Acción: show
df.show()

# Acción: count
total = df.count()
print("Total registros:", total)

# Acción: collect (⚠️ cuidado en prod)
data = df.collect()
print("Datos en driver:", data)
