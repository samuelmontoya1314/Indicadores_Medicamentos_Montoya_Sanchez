"""Fase 7 - Capa rápida (Lambda) con Spark Structured Streaming.

Lee el tópico de Kafka como flujo y cuenta mensajes por ventana de tiempo de
30 segundos y por tipo de rol (FABRICANTE / IMPORTADOR), usando:
  - watermark de 2 minutos sobre la marca de tiempo del mensaje en Kafka
    (los eventos que lleguen con más retraso se ignoran y el estado viejo se libera);
  - checkpoint en HDFS: guarda los offsets ya procesados y el estado de la agregación.
    Al reiniciar, el flujo continúa desde donde quedó: NO reprocesa ni duplica.

Cada micro-lote se escribe en SQLite (zona de servicio, tabla stream_conteos_ventana)
con UPSERT por (ventana_inicio, tiporol), así que reescribir un lote es idempotente.

Se usa trigger(availableNow=True): procesa todo lo disponible y se detiene. Correrlo
otra vez equivale a reiniciar el flujo.

Uso:
    python src/06_streaming.py          # procesa lo nuevo y se detiene
    python src/06_streaming.py --reset  # borra checkpoint y tabla (empezar de cero)
"""
import argparse
import sqlite3
import sys

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

import config as C
from utils import ahora, guardar_evidencia, hdfs, hdfs_existe

ESQUEMA_MSG = StructType([
    StructField("id_registro", StringType()),
    StructField("fecha_ingesta", StringType()),
    StructField("payload", StructType([
        StructField("tiporol", StringType()),
        StructField("estadocum", StringType()),
        StructField("atc", StringType()),
    ])),
])

DDL = """
CREATE TABLE IF NOT EXISTS stream_conteos_ventana (
    ventana_inicio TEXT NOT NULL,
    ventana_fin    TEXT NOT NULL,
    tiporol        TEXT NOT NULL,
    mensajes       INTEGER NOT NULL,
    actualizado    TEXT NOT NULL,
    PRIMARY KEY (ventana_inicio, tiporol)
)"""


def totales_sqlite():
    con = sqlite3.connect(C.SERVICE_DB)
    con.execute(DDL)
    fila = con.execute("SELECT COUNT(*), COALESCE(SUM(mensajes),0) FROM stream_conteos_ventana").fetchone()
    con.close()
    return {"ventanas": fila[0], "mensajes": fila[1]}


def escribir_lote(batch_df, batch_id):
    filas = batch_df.collect()
    if not filas:
        return
    con = sqlite3.connect(C.SERVICE_DB)
    con.execute(DDL)
    marca = ahora().isoformat()
    con.executemany(
        """INSERT INTO stream_conteos_ventana VALUES (?,?,?,?,?)
           ON CONFLICT(ventana_inicio, tiporol) DO UPDATE SET
             mensajes=excluded.mensajes, actualizado=excluded.actualizado""",
        [(str(r["ventana"]["start"]), str(r["ventana"]["end"]), r["tiporol"],
          int(r["mensajes"]), marca) for r in filas])
    con.commit()
    con.close()
    print(f"   micro-lote {batch_id}: {len(filas)} ventana/s actualizada/s")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--reset", action="store_true")
    args = ap.parse_args()

    if args.reset:
        if hdfs_existe(C.HDFS_CHECKPOINT):
            hdfs("-rm", "-r", "-skipTrash", C.HDFS_CHECKPOINT)
        con = sqlite3.connect(C.SERVICE_DB)
        con.execute("DROP TABLE IF EXISTS stream_conteos_ventana")
        con.commit()
        con.close()
        print("Checkpoint y tabla de streaming reiniciados.")

    antes = totales_sqlite()
    checkpoint_previo = hdfs_existe(C.HDFS_CHECKPOINT)
    print(f"Checkpoint previo: {'sí (reinicio)' if checkpoint_previo else 'no (primera vez)'}")
    print(f"Antes  -> ventanas: {antes['ventanas']}, mensajes acumulados: {antes['mensajes']}")

    from spark_utils import get_spark
    spark = get_spark("fase7-streaming", kafka=True)

    flujo = (spark.readStream.format("kafka")
             .option("kafka.bootstrap.servers", C.KAFKA_BOOTSTRAP)
             .option("subscribe", C.TOPIC)
             .option("startingOffsets", "earliest")
             .load())

    eventos = (flujo
        .select(F.col("timestamp").alias("ts_kafka"),
                F.from_json(F.col("value").cast("string"), ESQUEMA_MSG).alias("m"))
        .select("ts_kafka",
                F.col("m.id_registro").alias("id_registro"),
                F.coalesce(F.upper(F.trim(F.col("m.payload.tiporol"))),
                           F.lit("NO REPORTA")).alias("tiporol")))

    conteos = (eventos
        .withWatermark("ts_kafka", "2 minutes")
        .groupBy(F.window("ts_kafka", "30 seconds").alias("ventana"), "tiporol")
        .agg(F.count("*").alias("mensajes")))

    consulta = (conteos.writeStream
        .outputMode("update")
        .foreachBatch(escribir_lote)
        .option("checkpointLocation", f"{C.HDFS_URI}{C.HDFS_CHECKPOINT}")
        .trigger(availableNow=True)
        .start())
    consulta.awaitTermination()

    filas_leidas = sum(p.get("numInputRows", 0) for p in consulta.recentProgress)
    lotes = len(consulta.recentProgress)
    spark.stop()

    despues = totales_sqlite()
    evidencia = {
        "fecha": ahora().isoformat(),
        "checkpoint_previo": checkpoint_previo,
        "mensajes_leidos_de_kafka_en_esta_corrida": filas_leidas,
        "micro_lotes": lotes,
        "antes": antes,
        "despues": despues,
    }
    print(f"\nMensajes leídos de Kafka en esta corrida: {filas_leidas} ({lotes} micro-lote/s)")
    print(f"Después -> ventanas: {despues['ventanas']}, mensajes acumulados: {despues['mensajes']}")
    if checkpoint_previo and filas_leidas == 0:
        print("Reinicio verificado: el checkpoint evitó reprocesar mensajes ya leídos.")
    nombre = "streaming_reinicio.json" if checkpoint_previo else "streaming_primera_corrida.json"
    guardar_evidencia(nombre, evidencia)
    return 0


if __name__ == "__main__":
    sys.exit(main())

