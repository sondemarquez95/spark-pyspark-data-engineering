from pyspark.sql import SparkSession

# Crear SparkSession
# SparkSession es el punto de entrada a Spark
spark = SparkSession.builder \
    .appName("SparkBasicsExample") \
    .master("local[*]") \
    .getOrCreate()

# Verificar que Spark está activo
print("Spark version:", spark.version)

# Mostrar configuración básica
print("App Name:", spark.sparkContext.appName)
print("Master:", spark.sparkContext.master)
