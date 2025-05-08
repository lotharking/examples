import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import length
import time

# Crear SparkSession para PySpark
spark = SparkSession.builder \
    .appName("ComparacionTiempoEjecucion") \
    .getOrCreate()

# Ruta del archivo CSV
file_path = "data.csv"

# --- Pandas: procesamiento ---
start_time_pandas = time.time()

# Cargar el archivo CSV con Pandas
df_pandas = pd.read_csv(file_path)

# Filtrar textos con longitud mayor a 10 caracteres y agrupar por la columna 'short'
df_pandas_filtered = df_pandas[df_pandas['original'].str.len() > 10]
df_pandas_grouped = df_pandas_filtered.groupby('short').size()

end_time_pandas = time.time()
pandas_time = end_time_pandas - start_time_pandas
print(f"Tiempo de ejecución con Pandas: {pandas_time:.4f} segundos")
print(df_pandas_grouped)

# --- PySpark: procesamiento ---
start_time_spark = time.time()

# Cargar el archivo CSV con PySpark
df_spark = spark.read.csv(file_path, header=True, inferSchema=True)

# Filtrar textos con longitud mayor a 10 caracteres y agrupar por 'short'
df_spark_filtered = df_spark.filter(length(df_spark['original']) > 10)
df_spark_grouped = df_spark_filtered.groupBy("short").count()

# Mostrar el resultado para que PySpark ejecute
df_spark_grouped.show()

end_time_spark = time.time()
spark_time = end_time_spark - start_time_spark
print(f"Tiempo de ejecución con PySpark: {spark_time:.4f} segundos")

# Cerrar la sesión de Spark
spark.stop()
