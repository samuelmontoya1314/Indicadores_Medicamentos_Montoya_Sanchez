"""Fase 2 - Ingesta a la zona raw.

Descarga el dataset CUM desde la API Socrata de datos.gov.co con paginación y
reintentos, envuelve cada registro en un «sobre» JSON sin modificarlo y lo guarda
en JSON Lines particionado por fecha_ingesta=AAAA-MM-DD. Cada ejecución deja un
manifiesto con la URL, los parámetros, las páginas y el conteo.

Carga incremental: si existe una marca de agua (último :updated_at cargado con
éxito) solo se piden los registros con :updated_at posterior.

Uso:
    python src/01_ingesta.py            # incremental (o completa la primera vez)
    python src/01_ingesta.py --completa # ignora la marca de agua
    python src/01_ingesta.py --limite 1000  # prueba rápida
"""
import argparse
import hashlib
import json
import os
import sys
import time

import requests

import config as C
from utils import ahora, escribir_json, leer_json


def leer_watermark():
    return leer_json(C.WATERMARK_FILE, {}).get("watermark")


def get_con_reintentos(sesion, params):
    """GET con reintentos y espera exponencial ante errores transitorios."""
    espera = 2
    for intento in range(1, C.MAX_REINTENTOS + 1):
        try:
            r = sesion.get(C.API_URL, params=params, timeout=C.TIMEOUT_S)
            if r.status_code == 200:
                return r.json(), intento
            if r.status_code in (429, 500, 502, 503, 504):
                print(f"   HTTP {r.status_code}: reintento {intento}/{C.MAX_REINTENTOS} en {espera}s")
            else:
                r.raise_for_status()  # error 4xx no transitorio: no tiene sentido reintentar
        except (requests.ConnectionError, requests.Timeout) as e:
            print(f"   {type(e).__name__}: reintento {intento}/{C.MAX_REINTENTOS} en {espera}s")
        time.sleep(espera)
        espera *= 2
    raise RuntimeError(f"La API no respondió tras {C.MAX_REINTENTOS} intentos")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--completa", action="store_true", help="ignorar la marca de agua")
    ap.add_argument("--limite", type=int, default=None, help="máximo de registros (pruebas)")
    args = ap.parse_args()

    inicio = ahora()
    run_id = inicio.strftime("%Y%m%dT%H%M%S")
    fecha_ingesta = inicio.strftime("%Y-%m-%d")
    watermark = None if args.completa else leer_watermark()

    # SoQL exige que el * vaya primero; los campos de sistema (:id, :updated_at)
    # no vienen por defecto y hay que pedirlos explícitamente.
    params_base = {"$select": f"*,{C.CAMPO_ID},:created_at,{C.CAMPO_WATERMARK}"}
    if watermark:
        # la API devuelve '...Z' pero el filtro SoQL espera el timestamp sin la Z
        params_base["$where"] = f"{C.CAMPO_WATERMARK} > '{watermark.rstrip('Z')}'"
        params_base["$order"] = f"{C.CAMPO_WATERMARK},{C.CAMPO_ID}"
        print(f"Carga INCREMENTAL: registros con {C.CAMPO_WATERMARK} > {watermark}")
    else:
        params_base["$order"] = C.CAMPO_ID  # orden estable para paginar sin saltos
        print("Carga COMPLETA (no hay marca de agua)")

    sesion = requests.Session()
    if C.APP_TOKEN:
        sesion.headers["X-App-Token"] = C.APP_TOKEN

    carpeta = os.path.join(C.RAW_DIR, f"fecha_ingesta={fecha_ingesta}")
    os.makedirs(carpeta, exist_ok=True)
    archivo = os.path.join(carpeta, f"run_{run_id}.jsonl")

    total, offset, paginas, max_wm, sin_id = 0, 0, [], watermark, 0
    tamano_pagina = min(C.PAGE_SIZE, args.limite) if args.limite else C.PAGE_SIZE
    with open(archivo, "w", encoding="utf-8") as f:
        while True:
            params = {**params_base, "$limit": tamano_pagina, "$offset": offset}
            t0 = time.time()
            registros, intentos = get_con_reintentos(sesion, params)
            paginas.append({"offset": offset, "registros": len(registros),
                            "intentos": intentos, "segundos": round(time.time() - t0, 2)})
            print(f"  página offset={offset:>7}: {len(registros):>6} registros ({intentos} intento/s)")
            for rec in registros:
                id_registro = rec.get(C.CAMPO_ID)
                if not id_registro:  # respaldo: hash del registro original
                    sin_id += 1
                    id_registro = hashlib.sha256(
                        json.dumps(rec, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
                sobre = {
                    "fuente": C.FUENTE,
                    "dataset": C.DATASET,
                    "id_registro": id_registro,
                    "fecha_ingesta": fecha_ingesta,
                    "ts_ingesta": inicio.isoformat(),
                    "run_id": run_id,
                    "payload": rec,          # registro original intacto
                }
                f.write(json.dumps(sobre, ensure_ascii=False) + "\n")
                wm = rec.get(C.CAMPO_WATERMARK)
                if wm and (max_wm is None or wm > max_wm):
                    max_wm = wm
            total += len(registros)
            offset += len(registros)
            if len(registros) < tamano_pagina or (args.limite and total >= args.limite):
                break

    if total == 0:
        os.remove(archivo)
        if not os.listdir(carpeta):
            os.rmdir(carpeta)
        archivo = None
        print("No hay registros nuevos desde la última marca de agua.")

    fin = ahora()
    manifiesto = {
        "run_id": run_id,
        "fuente": C.FUENTE,
        "dataset": C.DATASET,
        "nombre": C.NOMBRE_DATASET,
        "url": C.API_URL,
        "parametros": params_base,
        "tamano_pagina": tamano_pagina,
        "tipo_carga": "incremental" if watermark else "completa",
        "watermark_anterior": watermark,
        "watermark_nuevo": max_wm,   # se confirma solo si el pipeline termina bien (fase 5)
        "paginas": paginas,
        "total_registros": total,
        "registros_sin_id": sin_id,
        "archivo": archivo,
        "bytes": os.path.getsize(archivo) if archivo else 0,
        "inicio": inicio.isoformat(),
        "fin": fin.isoformat(),
        "duracion_s": round((fin - inicio).total_seconds(), 1),
    }
    ruta_man = os.path.join(C.MANIFEST_DIR, f"manifest_{run_id}.json")
    escribir_json(ruta_man, manifiesto)
    escribir_json(os.path.join(C.STATE_DIR, "ultimo_manifiesto.json"), {"ruta": ruta_man})
    print(f"\nTotal descargado: {total} registros en {len(paginas)} página/s "
          f"({manifiesto['duracion_s']} s)")
    print(f"Archivo raw : {archivo}")
    print(f"Manifiesto  : {ruta_man}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
