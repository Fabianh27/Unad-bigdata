from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType

# Crear sesión de Spark
spark = SparkSession.builder \
    .appName("StreamingTerremotos") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Esquema de los datos JSON recibidos desde Kafka
schema = StructType() \
    .add("id", StringType()) \
    .add("magnitude", DoubleType()) \
    .add("depth", DoubleType()) \
    .add("location", StringType()) \
    .add("tsunami", StringType())

# Leer flujo desde Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "terremotos_topic") \
    .option("startingOffsets", "earliest") \
    .load()

# Convertir los mensajes de Kafka (binarios) a JSON
json_df = df.selectExpr("CAST(value AS STRING)")

data_df = json_df.select(from_json(col("value"), schema).alias("data")).select("data.*")

# Agregar columna "riesgo" como cálculo derivado
resultados_df = data_df.withColumn(
    "riesgo",
    (col("magnitude") * col("depth") / 100).cast("double")
)

# Mostrar los resultados en consola
query = resultados_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()