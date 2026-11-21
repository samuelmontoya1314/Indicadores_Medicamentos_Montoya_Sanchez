"""Funciones auxiliares compartidas: JSON, HDFS por línea de comandos y evidencias."""
import glob
import json
import os
import subprocess
from datetime import datetime
from zoneinfo import ZoneInfo

import config as C

TZ = ZoneInfo("America/Bogota")


def ahora():
    return datetime.now(TZ)


def leer_json(ruta, defecto=None):
    if os.path.exists(ruta):
        with open(ruta, encoding="utf-8") as f:
            return json.load(f)
    return defecto


def escribir_json(ruta, obj):
    os.makedirs(os.path.dirname(ruta), exist_ok=True)
    with open(ruta, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2, default=str)


def guardar_evidencia(nombre, contenido):
    """Guarda un texto o un objeto JSON en la carpeta evidencias/."""
    ruta = os.path.join(C.EVIDENCIAS_DIR, nombre)
    if isinstance(contenido, (dict, list)):
        escribir_json(ruta, contenido)
    else:
        with open(ruta, "w", encoding="utf-8") as f:
            f.write(str(contenido))
    return ruta


# ---------------- HDFS (vía CLI, como en el laboratorio) ----------------
def _hdfs_bin():
    home = os.environ.get("HADOOP_HOME")
    return os.path.join(home, "bin", "hdfs") if home else "hdfs"


def hdfs(*args, check=True, capturar=True):
    """Ejecuta `hdfs dfs <args>` y devuelve la salida estándar."""
    cmd = [_hdfs_bin(), "dfs", *args]
    r = subprocess.run(cmd, capture_output=capturar, text=True)
    if check and r.returncode != 0:
        raise RuntimeError(f"Falló {' '.join(cmd)}:\n{r.stderr}")
    return r.stdout if capturar else ""


def hdfs_existe(ruta):
    return subprocess.run([_hdfs_bin(), "dfs", "-test", "-e", ruta]).returncode == 0


def hdfs_du_bytes(ruta):
    """Tamaño total (bytes) de una ruta HDFS."""
    if not hdfs_existe(ruta):
        return 0
    salida = hdfs("-du", "-s", ruta).strip()
    return int(salida.split()[0]) if salida else 0


def hdfs_contar_lineas(patron):
    """Cuenta líneas de los archivos que cumplen el patrón (glob HDFS)."""
    p1 = subprocess.Popen([_hdfs_bin(), "dfs", "-cat", patron],
                          stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    n = sum(1 for _ in p1.stdout)
    p1.wait()
    return n


# ---------------- Zona raw local ----------------
def archivos_raw_locales():
    return sorted(glob.glob(os.path.join(C.RAW_DIR, "fecha_ingesta=*", "*.jsonl")))


def ultimo_manifiesto():
    ruta = leer_json(os.path.join(C.STATE_DIR, "ultimo_manifiesto.json"), {}).get("ruta")
    return leer_json(ruta) if ruta else None


def tamano_dir_local(ruta):
    total = 0
    for raiz, _, archivos in os.walk(ruta):
        for a in archivos:
            total += os.path.getsize(os.path.join(raiz, a))
    return total


def mb(b):
    return round(b / 1024 / 1024, 2)

