from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("LazyEvaluation") \
    .getOrCreate()

data = [(1, 100), (2, 200), (3, 300)]
df = spark.createDataFrame(data, ["id", "valor"])

# Transformación (NO se ejecuta aún)
df_filtered = df.filter(df.valor > 150)

print("Esta línea se ejecuta, pero Spark NO ha procesado nada todavía")

# Acción (AQUÍ Spark ejecuta el DAG)
df_filtered.show()

# Otra acción
total = df_filtered.count()
print("Total registros filtrados:", total)
