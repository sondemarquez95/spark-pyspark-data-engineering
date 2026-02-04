from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ReadFiles") \
    .getOrCreate()

# =====================
# Leer CSV
# =====================
df_csv = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("data/ventas.csv")

df_csv.show()

# =====================
# Leer Parquet (recomendado)
# =====================
df_parquet = spark.read.parquet("data/ventas.parquet")
df_parquet.show()

# =====================
# Leer JSON
# =====================
df_json = spark.read.json("data/ventas.json")
df_json.show()
