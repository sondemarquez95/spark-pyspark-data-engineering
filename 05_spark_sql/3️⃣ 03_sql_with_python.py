from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("SparkSQLWithPython") \
    .getOrCreate()

# Ejecutar query SQL
df_result = spark.sql("""
    SELECT
        id_cliente,
        SUM(monto) AS total_ventas
    FROM ventas
    GROUP BY id_cliente
""")

df_result.show()
