"""Fase 3 - Productor Kafka.

Crea (si no existe) el tópico con 3 particiones y publica cada línea del raw como un
mensaje con key = id_registro. La key garantiza que un mismo registro siempre cae en
la misma partición (orden por registro).

Cómo se evitan duplicados:
  1. enable.idempotence=True + acks=all: los reintentos internos del productor no
     duplican mensajes en el broker.
  2. state/archivos_publicados.json: un archivo raw ya publicado no se vuelve a
     publicar si el productor se ejecuta dos veces (salvo con --forzar).
  3. Aun si se forzara la doble publicación (Kafka es «al menos una vez»), la zona
     curada deduplica por id_registro (fase 5), así que el resultado final no cambia.

Uso:
    python src/02_productor.py              # publica el archivo de la última ingesta
    python src/02_productor.py --todos      # todos los archivos raw no publicados
    python src/02_productor.py --forzar     # republica aunque ya se haya publicado (demo)
"""
import argparse
import json
import os
import sys
from collections import Counter

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

import config as C
from utils import (ahora, archivos_raw_locales, escribir_json, guardar_evidencia,
                   leer_json, ultimo_manifiesto)


def crear_topico():
    admin = AdminClient({"bootstrap.servers": C.KAFKA_BOOTSTRAP})
    existentes = admin.list_topics(timeout=15).topics
    if C.TOPIC in existentes:
        n = len(existentes[C.TOPIC].partitions)
        print(f"Tópico '{C.TOPIC}' ya existe con {n} particiones")
        return
    futuros = admin.create_topics([NewTopic(C.TOPIC, num_partitions=C.PARTICIONES,
                                            replication_factor=1)])
    for t, f in futuros.items():
        f.result()
        print(f"Tópico '{t}' creado con {C.PARTICIONES} particiones")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--todos", action="store_true")
    ap.add_argument("--forzar", action="store_true")
    args = ap.parse_args()

    crear_topico()

    if args.todos:
        archivos = archivos_raw_locales()
    else:
        man = ultimo_manifiesto()
        archivos = [man["archivo"]] if man and man.get("archivo") else []

    publicados = set(leer_json(C.PUBLICADOS_FILE, []))
    pendientes = [a for a in archivos if args.forzar or os.path.basename(a) not in publicados]
    omitidos = len(archivos) - len(pendientes)
    if omitidos:
        print(f"{omitidos} archivo/s omitido/s porque ya se habían publicado")
    if not pendientes:
        print("No hay archivos nuevos para publicar.")
        guardar_evidencia("kafka_productor_ultima.json",
                          {"fecha": ahora().isoformat(), "mensajes": 0, "omitidos": omitidos})
        return 0

    productor = Producer({
        "bootstrap.servers": C.KAFKA_BOOTSTRAP,
        "enable.idempotence": True,   # implica acks=all y reintentos sin duplicar
        "acks": "all",
        "linger.ms": 50,
        "compression.type": "lz4",
    })
    por_particion, errores = Counter(), Counter()

    def al_entregar(err, msg):
        if err:
            errores[str(err)] += 1
        else:
            por_particion[msg.partition()] += 1

    total = 0
    for ruta in pendientes:
        n_arch = 0
        with open(ruta, encoding="utf-8") as f:
            for linea in f:
                linea = linea.rstrip("\n")
                if not linea:
                    continue
                # key = id_registro (se extrae sin re-serializar la línea: el raw no se toca)
                key = json.loads(linea)["id_registro"]
                while True:
                    try:
                        productor.produce(C.TOPIC, key=key.encode(), value=linea.encode(),
                                          on_delivery=al_entregar)
                        break
                    except BufferError:   # cola local llena: esperar entregas
                        productor.poll(0.5)
                productor.poll(0)
                n_arch += 1
        productor.flush()
        publicados.add(os.path.basename(ruta))
        total += n_arch
        print(f"  {os.path.basename(ruta)}: {n_arch} mensajes publicados")

    productor.flush()
    escribir_json(C.PUBLICADOS_FILE, sorted(publicados))
    resumen = {
        "fecha": ahora().isoformat(),
        "topico": C.TOPIC,
        "mensajes": total,
        "por_particion": {str(k): v for k, v in sorted(por_particion.items())},
        "errores": dict(errores),
        "forzado": args.forzar,
    }
    guardar_evidencia("kafka_productor_ultima.json", resumen)
    print(f"\nTotal publicados: {total} | por partición: {resumen['por_particion']}")
    if errores:
        print(f"ERRORES de entrega: {dict(errores)}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
