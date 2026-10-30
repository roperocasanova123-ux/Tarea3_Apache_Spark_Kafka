"""
Tarea 3 - Big Data: procesamiento por lotes con Apache Spark (PySpark)
Proyecto: análisis de ventas de una tienda de tecnología.
"""

from pathlib import Path
import csv

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, year, month, sum as Fsum,
    count, avg, round as Fround
)

ROOT = Path(__file__).resolve().parents[1]
CSV_PATH = ROOT / "datos" / "ventas_tienda.csv"
OUT = ROOT / "resultados_batch"
OUT.mkdir(exist_ok=True)


def guardar_csv(dataframe, nombre_archivo):
    """Guarda los resultados calculados por Spark utilizando Python."""
    destino = OUT / nombre_archivo
    filas = dataframe.collect()

    with open(destino, "w", newline="", encoding="utf-8-sig") as archivo:
        escritor = csv.writer(archivo)
        escritor.writerow(dataframe.columns)

        for fila in filas:
            escritor.writerow(list(fila))

    print(f"Archivo guardado: {destino}")


spark = (
    SparkSession.builder
    .appName("Tarea3_AnalisisVentas")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

try:
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(str(CSV_PATH))
    )

    print("\n=== ESQUEMA ORIGINAL ===")
    df.printSchema()

    print("\n=== MUESTRA ORIGINAL ===")
    df.show(10, truncate=False)

    total_original = df.count()
    print(f"Filas originales: {total_original}")

    clean = (
        df.withColumn("fecha", to_date(col("fecha"), "yyyy-MM-dd"))
        .withColumn("cantidad", col("cantidad").cast("integer"))
        .withColumn("precio_unitario", col("precio_unitario").cast("double"))
        .filter(col("venta_id").isNotNull())
        .filter(col("fecha").isNotNull())
        .filter(col("producto").isNotNull())
        .filter(col("cantidad").isNotNull() & (col("cantidad") > 0))
        .filter(
            col("precio_unitario").isNotNull()
            & (col("precio_unitario") > 0)
        )
        .fillna({
            "ciudad": "No especificada",
            "canal": "No especificado"
        })
        .withColumn(
            "ingreso",
            Fround(col("cantidad") * col("precio_unitario"), 2)
        )
        .withColumn("anio", year(col("fecha")))
        .withColumn("mes", month(col("fecha")))
    )

    total_limpio = clean.count()

    print("\n=== RESUMEN DE LIMPIEZA ===")
    print(f"Filas después de limpieza: {total_limpio}")
    print(f"Filas descartadas: {total_original - total_limpio}")

    print("\n=== INGRESOS POR PRODUCTO ===")

    by_product = (
        clean.groupBy("producto")
        .agg(
            count("*").alias("numero_ventas"),
            Fround(Fsum("cantidad"), 0).alias("unidades"),
            Fround(Fsum("ingreso"), 2).alias("ingresos_totales")
        )
        .orderBy(col("ingresos_totales").desc())
    )

    by_product.show(truncate=False)
    guardar_csv(by_product, "ingresos_por_producto.csv")

    print("\n=== INGRESOS POR CIUDAD ===")

    by_city = (
        clean.groupBy("ciudad")
        .agg(
            Fround(Fsum("ingreso"), 2).alias("ingresos_totales")
        )
        .orderBy(col("ingresos_totales").desc())
    )

    by_city.show(truncate=False)
    guardar_csv(by_city, "ingresos_por_ciudad.csv")

    print("\n=== INGRESOS POR CANAL ===")

    by_channel = (
        clean.groupBy("canal")
        .agg(
            Fround(Fsum("ingreso"), 2).alias("ingresos_totales")
        )
        .orderBy(col("ingresos_totales").desc())
    )

    by_channel.show(truncate=False)
    guardar_csv(by_channel, "ingresos_por_canal.csv")

    print("\n=== ESTADÍSTICAS GENERALES ===")

    estadisticas = clean.select(
        Fround(Fsum("ingreso"), 2).alias("ingresos_totales"),
        count("*").alias("ventas_validas"),
        Fround(avg("ingreso"), 2).alias("ingreso_promedio")
    )

    estadisticas.show()
    guardar_csv(estadisticas, "estadisticas_generales.csv")

    print("\nPROCESAMIENTO POR LOTES FINALIZADO CORRECTAMENTE.")
    print(f"Resultados CSV guardados en: {OUT}")

finally:
    spark.stop()