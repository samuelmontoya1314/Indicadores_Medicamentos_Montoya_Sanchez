"""Orquestador: corre el pipeline completo (fases 2 a 8) y registra conteos.

Sirve para demostrar la idempotencia (fase 6): si se ejecuta dos veces seguidas, la
segunda corrida no trae registros nuevos (marca de agua), no publica nada en Kafka y
los conteos de la zona curada y de servicio quedan iguales.

Uso:
    python src/run_pipeline.py --etiqueta corrida_1
    python src/run_pipeline.py --etiqueta corrida_2
    PP_SIMULAR_FALLO=1 python src/run_pipeline.py --etiqueta fallo   # se detiene en calidad
"""
import argparse
import json
import os
import sqlite3
import subprocess
import sys
import time

import config as C
from utils import (ahora, archivos_raw_locales, hdfs_contar_lineas, leer_json,
                   ultimo_manifiesto)

AQUI = os.path.dirname(os.path.abspath(__file__))
PASOS = [
    ("Fase 2   - Ingesta API -> raw", "01_ingesta.py"),
    ("Fase 3   - Productor Kafka", "02_productor.py"),
    ("Fase 3/4 - Consumidor Kafka -> HDFS", "03_consumidor_hdfs.py"),
    ("Fase 4   - MapReduce vs Spark", "04_mapreduce.py"),
    ("Fase 5/6 - Zona curada + calidad", "05_curada_spark.py"),
    ("Fase 7   - Streaming (capa rápida)", "06_streaming.py"),
    ("Fase 8   - Zona de servicio", "07_servicio.py"),
    ("Fase 8   - Tablero", "08_tablero.py"),
]
REGISTRO = os.path.join(C.EVIDENCIAS_DIR, "idempotencia_corridas.jsonl")


def offsets_kafka():
    from confluent_kafka import Consumer, TopicPartition
    c = Consumer({"bootstrap.servers": C.KAFKA_BOOTSTRAP, "group.id": "conteo-offsets"})
    md = c.list_topics(C.TOPIC, timeout=10).topics[C.TOPIC]
    total = sum(c.get_watermark_offsets(TopicPartition(C.TOPIC, p), timeout=10)[1]
                for p in md.partitions)
    c.close()
    return total


def conteos(etiqueta, segundos):
    man = ultimo_manifiesto() or {}
    raw_local = 0
    for a in archivos_raw_locales():
        with open(a, encoding="utf-8") as f:
            raw_local += sum(1 for _ in f)
    calidad = leer_json(os.path.join(C.CALIDAD_DIR, "reporte_calidad_ultimo.json"), {})
    con = sqlite3.connect(C.SERVICE_DB)
    cum_total = con.execute("SELECT cum_total FROM ind_resumen").fetchone()[0]
    stream = con.execute("SELECT COALESCE(SUM(mensajes),0) FROM stream_conteos_ventana").fetchone()[0]
    con.close()
    return {
        "etiqueta": etiqueta,
        "fecha": ahora().isoformat(timespec="seconds"),
        "tipo_carga": man.get("tipo_carga"),
        "registros_nuevos_api": man.get("total_registros"),
        "lineas_raw_local": raw_local,
        "mensajes_en_kafka": offsets_kafka(),
        "lineas_raw_hdfs": hdfs_contar_lineas(f"{C.HDFS_RAW}/fecha_ingesta=*/*.jsonl"),
        "filas_curadas": calidad.get("filas_curadas"),
        "cum_en_servicio": cum_total,
        "mensajes_streaming": stream,
        "watermark": leer_json(C.WATERMARK_FILE, {}).get("watermark"),
        "duracion_s": round(segundos, 1),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--etiqueta", default=ahora().strftime("corrida_%H%M%S"))
    ap.add_argument("--sin-mapreduce", action="store_true", help="omitir la fase 4 (más rápido)")
    args = ap.parse_args()

    t0 = time.time()
    for nombre, script in PASOS:
        if args.sin_mapreduce and script == "04_mapreduce.py":
            continue
        print(f"\n{'=' * 70}\n>> {nombre}\n{'=' * 70}", flush=True)
        r = subprocess.run([sys.executable, os.path.join(AQUI, script)])
        if r.returncode != 0:
            print(f"\nPIPELINE DETENIDO en '{nombre}' (código {r.returncode}). "
                  "Los pasos siguientes no se ejecutaron.")
            return r.returncode

    fila = conteos(args.etiqueta, time.time() - t0)
    with open(REGISTRO, "a", encoding="utf-8") as f:
        f.write(json.dumps(fila, ensure_ascii=False) + "\n")

    import pandas as pd
    with open(REGISTRO, encoding="utf-8") as f:
        historial = pd.DataFrame([json.loads(l) for l in f if l.strip()])
    print(f"\n{'=' * 70}\nConteos por corrida (evidencias/idempotencia_corridas.jsonl)\n{'=' * 70}")
    with pd.option_context("display.max_columns", None, "display.width", 200):
        print(historial.drop(columns=["fecha"]).tail(6).to_string(index=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())

