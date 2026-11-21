"""Fase 3/4 - Consumidor Kafka -> HDFS (data lake raw).

Lee el tópico con el grupo de consumidores `cum-hdfs`, acumula los mensajes en lotes
y escribe UN archivo por lote (no uno por mensaje) en:
    /datalake/raw/datos.gov.co/i7cb-raxc/fecha_ingesta=AAAA-MM-DD/lote_<ts>_<n>.jsonl

Los offsets se confirman (commit) solo después de que el lote quedó en HDFS:
si el proceso se cae a mitad de camino, el lote se vuelve a leer (al menos una vez)
y el posible duplicado se elimina en la zona curada por id_registro.

Termina solo cuando no llegan mensajes durante IDLE_SEGUNDOS (modo batch).
"""
import json
import os
import sys
import time
from collections import defaultdict

from confluent_kafka import Consumer, KafkaError

import config as C
from utils import ahora, guardar_evidencia, hdfs


def escribir_lote(lineas, n_lote, sello, resumen):
    por_fecha = defaultdict(list)
    for l in lineas:
        por_fecha[json.loads(l)["fecha_ingesta"]].append(l)
    for fecha, ls in por_fecha.items():
        nombre = f"lote_{sello}_{n_lote:04d}.jsonl"
        local = os.path.join(C.TMP_DIR, nombre)
        with open(local, "w", encoding="utf-8") as f:
            f.write("\n".join(ls) + "\n")
        destino = f"{C.HDFS_RAW}/fecha_ingesta={fecha}"
        hdfs("-mkdir", "-p", destino)
        hdfs("-put", "-f", local, f"{destino}/{nombre}")
        os.remove(local)
        resumen["archivos"].append({"ruta": f"{destino}/{nombre}", "mensajes": len(ls)})
        print(f"  lote {n_lote:04d} -> {destino}/{nombre} ({len(ls)} mensajes)")


def main():
    consumidor = Consumer({
        "bootstrap.servers": C.KAFKA_BOOTSTRAP,
        "group.id": C.GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,     # commit manual tras escribir en HDFS
    })
    consumidor.subscribe([C.TOPIC])
    sello = ahora().strftime("%Y%m%dT%H%M%S")
    buffer, n_lote, total = [], 0, 0
    por_particion = defaultdict(int)
    resumen = {"fecha": ahora().isoformat(), "grupo": C.GROUP_ID, "archivos": []}
    inicio = ultimo = time.time()
    recibio = False

    try:
        while True:
            msgs = consumidor.consume(num_messages=5000, timeout=1.0)
            if not msgs:
                espera = C.IDLE_SEGUNDOS if recibio else 30
                if time.time() - (ultimo if recibio else inicio) > espera:
                    break
                continue
            for m in msgs:
                if m.error():
                    if m.error().code() != KafkaError._PARTITION_EOF:
                        print("Error:", m.error())
                    continue
                buffer.append(m.value().decode("utf-8"))
                por_particion[m.partition()] += 1
            recibio, ultimo = True, time.time()
            if len(buffer) >= C.LOTE_HDFS:
                n_lote += 1
                escribir_lote(buffer, n_lote, sello, resumen)
                consumidor.commit(asynchronous=False)
                total += len(buffer)
                buffer = []
        if buffer:
            n_lote += 1
            escribir_lote(buffer, n_lote, sello, resumen)
            consumidor.commit(asynchronous=False)
            total += len(buffer)
    finally:
        consumidor.close()

    resumen.update({"mensajes": total, "lotes": n_lote,
                    "por_particion": {str(k): v for k, v in sorted(por_particion.items())}})
    guardar_evidencia("kafka_consumidor_ultima.json", resumen)
    print(f"\nConsumidos {total} mensajes en {n_lote} lote/s | por partición: "
          f"{resumen['por_particion']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

