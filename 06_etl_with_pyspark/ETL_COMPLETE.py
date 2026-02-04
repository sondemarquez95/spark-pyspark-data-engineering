from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def extract_data(spark):
    """
    Extrae datos desde archivos Parquet (caso típico en Big Data)
    """

    df = spark.read.parquet("data/ventas.parquet")

    print("Datos extraídos correctamente")
    return df

# --------------------------------

def transform_data(df):
    """
    Aplica reglas de transformación al DataFrame
    """

    # Filtrar registros inválidos
    df = df.filter(col("monto").isNotNull())

    # Crear columna calculada
    df = df.withColumn("monto_con_iva", col("monto") * 1.19)

    # Eliminar columnas innecesarias
    df = df.drop("moneda")

    return df

# --------------------------------
def load_data(df):
    """
    Guarda los datos transformados en Parquet
    """

    df.write \
        .mode("overwrite") \
        .partitionBy("anio") \
        .parquet("output/ventas_etl")

    print("Datos cargados correctamente")


# --------------------------------  
from pyspark.sql import SparkSession
from extract import extract_data
from transform import transform_data
from load import load_data

def run_etl():
    spark = SparkSession.builder \
        .appName("ETL_PySpark") \
        .getOrCreate()

    df_raw = extract_data(spark)
    df_clean = transform_data(df_raw)
    load_data(df_clean)

    spark.stop()
    print("ETL finalizada correctamente")

if __name__ == "__main__":
    run_etl()


