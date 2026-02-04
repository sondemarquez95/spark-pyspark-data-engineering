
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("BasicDataFrame") \
    .getOrCreate()

# Datos simulados (como si vinieran de una BD o archivo)
data = [
    (1, "Ana", 1200),
    (2, "Luis", 800),
    (3, "Carlos", 1500),
    (4, "Ana", 1200)
]

columns = ["id", "nombre", "monto"]

# Crear DataFrame
df = spark.createDataFrame(data, columns)

# Mostrar datos
df.show()

# Ver esquema
df.printSchema()

# Seleccionar columnas
df.select("nombre", "monto").show()

# Filtrar datos
df.filter(df.monto > 1000).show()
