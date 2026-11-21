"""Fase 8a - Zona de servicio: indicadores listos para el tablero en SQLite.

Lee la zona curada (Parquet en HDFS), calcula los indicadores que responden la
pregunta y los guarda en data/servicio/servicio.db. El tablero solo lee estas tablas
y no hace cálculos. Cada tabla se reemplaza completa en cada corrida (idempotente).

Unidad de análisis: el CUM (cum_id = expedientecum-consecutivocum), es decir, cada
presentación comercial. Una fila curada es CUM x principio activo x rol, por eso se
cuentan CUM distintos.
"""
import sqlite3
import sys

from pyspark.sql import functions as F

import config as C
from spark_utils import get_spark
from utils import ahora, guardar_evidencia

DDL_STREAM = """
CREATE TABLE IF NOT EXISTS stream_conteos_ventana (
    ventana_inicio TEXT NOT NULL, ventana_fin TEXT NOT NULL, tiporol TEXT NOT NULL,
    mensajes INTEGER NOT NULL, actualizado TEXT NOT NULL,
    PRIMARY KEY (ventana_inicio, tiporol))"""

VISTA_LAMBDA = """
CREATE VIEW v_lambda_tiporol AS
SELECT b.tiporol,
       b.registros AS registros_batch,
       COALESCE(s.mensajes, 0) AS mensajes_stream,
       COALESCE(s.mensajes, 0) - b.registros AS diferencia
FROM ind_registros_tiporol b
LEFT JOIN (SELECT tiporol, SUM(mensajes) AS mensajes
           FROM stream_conteos_ventana GROUP BY tiporol) s
       ON s.tiporol = b.tiporol"""


def main():
    spark = get_spark("fase8-servicio")
    cur = spark.read.parquet(f"{C.HDFS_URI}{C.HDFS_CURADA}").cache()
    activos = cur.filter("es_activo")
    tablas = {}

    # 1. CUM activos / inactivos por grupo ATC
    tablas["ind_atc_estado"] = (cur.groupBy("atc_nivel1", "grupo_atc").agg(
        F.countDistinct(F.when(F.col("es_activo"), F.col("cum_id"))).alias("cum_activos"),
        F.countDistinct(F.when(~F.col("es_activo"), F.col("cum_id"))).alias("cum_inactivos"),
        F.countDistinct("cum_id").alias("cum_total"))
        .orderBy(F.desc("cum_activos")))

    # 2. Dependencia de importación por grupo ATC (CUM activos con rol importador)
    tablas["ind_origen_atc"] = (activos.groupBy("atc_nivel1", "grupo_atc").agg(
        F.countDistinct("cum_id").alias("cum_activos"),
        F.countDistinct(F.when(F.col("es_importador"), F.col("cum_id"))).alias("cum_con_importador"),
        F.countDistinct(F.when(~F.col("es_importador"), F.col("cum_id"))).alias("cum_con_fabricante"))
        .withColumn("pct_importador", F.round(100 * F.col("cum_con_importador") / F.col("cum_activos"), 1))
        .orderBy(F.desc("pct_importador")))

    # 3. Concentración: top 15 titulares por CUM activos
    total_activos = activos.select("cum_id").distinct().count()
    top = (activos.groupBy("titular").agg(F.countDistinct("cum_id").alias("cum_activos"))
           .orderBy(F.desc("cum_activos"), "titular").limit(15).toPandas())
    top["pct_del_total"] = (100 * top["cum_activos"] / total_activos).round(2)
    top["pct_acumulado"] = top["pct_del_total"].cumsum().round(2)
    top.insert(0, "posicion", range(1, len(top) + 1))

    # 4. Evolución: registros sanitarios vigentes por año de expedición e inactivaciones por año
    exp = (cur.filter(F.col("anio_expedicion") >= 1990).groupBy(F.col("anio_expedicion").alias("anio"))
           .agg(F.countDistinct("expediente").alias("registros_expedidos")))
    ina = (cur.filter(F.col("fechainactivo").isNotNull())
           .groupBy(F.year("fechainactivo").alias("anio"))
           .agg(F.countDistinct("cum_id").alias("cum_inactivados")))
    tablas["ind_evolucion_anual"] = (exp.join(ina, "anio", "full").fillna(0)
                                     .filter(F.col("anio") >= 1990).orderBy("anio"))

    # 5. Vía de administración (CUM activos)
    tablas["ind_via_administracion"] = (activos.groupBy("viaadministracion")
        .agg(F.countDistinct("cum_id").alias("cum_activos")).orderBy(F.desc("cum_activos")))

    # 6. Registros (filas curadas) por tipo de rol: vista batch para unir con el streaming
    tablas["ind_registros_tiporol"] = cur.groupBy("tiporol").agg(F.count("*").alias("registros"))

    # 7. Resumen (KPIs) y fecha de corte
    cum_imp = activos.filter("es_importador").select("cum_id").distinct().count()
    fila = cur.agg(
        F.countDistinct("cum_id").alias("cum_total"),
        F.countDistinct(F.when(F.col("es_activo"), F.col("cum_id"))).alias("cum_activos"),
        F.countDistinct("expediente").alias("registros_sanitarios"),
        F.countDistinct(F.when(F.col("es_activo"), F.col("titular"))).alias("titulares_activos"),
        F.countDistinct("principioactivo").alias("principios_activos"),
        F.max("actualizado_fuente").alias("fecha_corte_fuente"),
        F.count("*").alias("filas_curadas")).first().asDict()
    pct = (top["cum_activos"] / total_activos)
    resumen = {
        **fila,
        "cum_inactivos": fila["cum_total"] - fila["cum_activos"],
        "pct_cum_activos_con_importador": round(100 * cum_imp / total_activos, 1),
        "pct_top10_titulares": round(100 * top["cum_activos"].head(10).sum() / total_activos, 1),
        "fuente": f"{C.NOMBRE_DATASET} ({C.FUENTE}, dataset {C.DATASET})",
        "url_fuente": C.API_URL,
        "fecha_proceso": ahora().isoformat(),
    }
    print("Resumen:", resumen)

    # ---------- Escritura en SQLite ----------
    import pandas as pd
    con = sqlite3.connect(C.SERVICE_DB)
    con.execute("DROP VIEW IF EXISTS v_lambda_tiporol")
    conteos = {}
    for nombre, sdf in tablas.items():
        pdf = sdf.toPandas()
        pdf.to_sql(nombre, con, if_exists="replace", index=False)
        conteos[nombre] = len(pdf)
    top.to_sql("ind_top_titulares", con, if_exists="replace", index=False)
    conteos["ind_top_titulares"] = len(top)
    pd.DataFrame([resumen]).to_sql("ind_resumen", con, if_exists="replace", index=False)
    conteos["ind_resumen"] = 1
    con.execute(DDL_STREAM)
    con.execute(VISTA_LAMBDA)
    con.commit()
    print("\nVista Lambda (batch vs. capa rápida):")
    for r in con.execute("SELECT * FROM v_lambda_tiporol"):
        print("  ", r)
    con.close()
    spark.stop()

    for t, n in conteos.items():
        print(f"  {t:<26} {n:>5} filas")
    guardar_evidencia("servicio_tablas.json", {"tablas": conteos, "resumen": resumen})
    return 0


if __name__ == "__main__":
    sys.exit(main())

