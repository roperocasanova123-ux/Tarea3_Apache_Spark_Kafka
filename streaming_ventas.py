"""
Consumidor de Kafka con Spark Structured Streaming.
Ejecutar desde la raíz del proyecto, con Kafka activo y topic creado:
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 codigo/streaming_ventas.py
IMPORTANTE: la versión del conector debe coincidir con la versión instalada de Spark.
Este ejemplo está preparado para Spark 3.5.1 / Scala 2.12.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window, sum as Fsum, count
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "ventas-tiempo-real"

schema = StructType([
    StructField("venta_id", StringType(), True),
    StructField("fecha_hora", StringType(), True),
    StructField("producto", StringType(), True),
    StructField("cantidad", IntegerType(), True),
    StructField("precio_unitario", DoubleType(), True),
    StructField("ciudad", StringType(), True)
])

spark = (SparkSession.builder.appName("Tarea3_StreamingVentas")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

try:
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
           .option("subscribe", TOPIC)
           .option("startingOffsets", "latest")
           .load())

    parsed = (raw.selectExpr("CAST(value AS STRING) AS json")
              .select(from_json(col("json"), schema).alias("v"))
              .select("v.*")
              .withColumn("fecha_hora", to_timestamp("fecha_hora"))
              .withColumn("ingreso", col("cantidad") * col("precio_unitario"))
              .filter(col("venta_id").isNotNull())
              .filter(col("cantidad") > 0)
              .filter(col("precio_unitario") > 0))

    # Conteo e ingresos agregados por ventana de un minuto y ciudad.
    resumen = (parsed.withWatermark("fecha_hora", "2 minutes")
               .groupBy(window(col("fecha_hora"), "1 minute"), col("ciudad"))
               .agg(count("*").alias("ventas"), Fsum("ingreso").alias("ingresos"))
               .selectExpr("window.start AS inicio_ventana",
                           "window.end AS fin_ventana", "ciudad", "ventas", "ingresos"))

    query = (resumen.writeStream
             .outputMode("update")
             .format("console")
             .option("truncate", "false")
             .option("checkpointLocation", "file:///tmp/checkpoint_tarea3_ventas")
             .start())
    print("Streaming activo. Inicie codigo/productor_kafka.py en otra terminal.")
    query.awaitTermination()
finally:
    spark.stop()
