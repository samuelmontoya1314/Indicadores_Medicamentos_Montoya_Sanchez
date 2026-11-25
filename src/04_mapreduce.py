"""Fase 4 - MapReduce sobre el raw en HDFS y comparación con Spark.

Cálculo: número de registros raw por grupo ATC nivel 1.
  - MapReduce: Hadoop Streaming con mapper.py y reducer.py (Python).
  - Spark: el mismo cálculo con DataFrames sobre los mismos archivos.
Al final se comparan ambos resultados clave por clave.
"""
import glob
import os
import subprocess
import sys

import config as C
from utils import guardar_evidencia, hdfs, hdfs_existe

AQUI = os.path.dirname(os.path.abspath(__file__))


def correr_mapreduce():
    home = os.environ["HADOOP_HOME"]
    jar = glob.glob(os.path.join(home, "share/hadoop/tools/lib/hadoop-streaming-*.jar"))[0]
    if hdfs_existe(C.HDFS_MR_OUT):
        hdfs("-rm", "-r", "-skipTrash", C.HDFS_MR_OUT)
    mapper = os.path.join(AQUI, "mapreduce", "mapper.py")
    reducer = os.path.join(AQUI, "mapreduce", "reducer.py")
    cmd = [os.path.join(home, "bin", "hadoop"), "jar", jar,
           "-files", f"{mapper},{reducer}",
           "-mapper", "python3 mapper.py",
           "-reducer", "python3 reducer.py",
           "-input", f"{C.HDFS_RAW}/fecha_ingesta=*/*.jsonl",
           "-output", C.HDFS_MR_OUT]
    print("Ejecutando:", " ".join(cmd))
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        print(r.stderr[-3000:])
        raise RuntimeError("Falló el job de MapReduce")
    salida = hdfs("-cat", f"{C.HDFS_MR_OUT}/part-*")
    resultado = {}
    for linea in salida.strip().splitlines():
        k, v = linea.split("\t")
        resultado[k] = int(v)
    return resultado


def correr_spark():
    from pyspark.sql import functions as F
    from spark_utils import get_spark, leer_raw_hdfs
    spark = get_spark("fase4-spark-vs-mapreduce")
    raw = leer_raw_hdfs(spark)
    atc = F.upper(F.trim(F.coalesce(F.col("payload.atc"), F.lit(""))))
    letra = F.substring(atc, 1, 1)
    grupo = F.when(atc.rlike(r"^[ABCDGHJLMNPRSV][0-9]{2}"), letra).otherwise(F.lit("SIN_ATC"))
    filas = raw.groupBy(grupo.alias("grupo")).count().collect()
    spark.stop()
    return {r["grupo"]: r["count"] for r in filas}


def main():
    mr = correr_mapreduce()
    sp = correr_spark()
    claves = sorted(set(mr) | set(sp))
    lineas = [f"{'grupo':<10}{'mapreduce':>12}{'spark':>12}{'coincide':>10}"]
    todo_ok = True
    for k in claves:
        ok = mr.get(k) == sp.get(k)
        todo_ok &= ok
        lineas.append(f"{k:<10}{mr.get(k, 0):>12}{sp.get(k, 0):>12}{'sí' if ok else 'NO':>10}")
    lineas.append(f"{'TOTAL':<10}{sum(mr.values()):>12}{sum(sp.values()):>12}"
                  f"{'sí' if sum(mr.values()) == sum(sp.values()) else 'NO':>10}")
    tabla = "\n".join(lineas)
    print(tabla)
    print("\nResultado:", "MapReduce y Spark COINCIDEN" if todo_ok else "HAY DIFERENCIAS")
    guardar_evidencia("mapreduce_vs_spark.txt", tabla + f"\n\nCoinciden: {todo_ok}\n")
    return 0 if todo_ok else 1


if __name__ == "__main__":
    sys.exit(main())

