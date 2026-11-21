"""Fase 5 - Zona curada con PySpark (+ calidad de la fase 6).

Lee los sobres raw desde HDFS, limpia y tipifica, valida la calidad y guarda Parquet
particionado por atc_nivel1. Cada decisión de limpieza queda registrada con el
número de filas antes y después (evidencias/curada_pasos.json).

Si una validación crítica falla, el script termina con error: no se escribe la zona
curada y NO se mueve la marca de agua (la próxima corrida vuelve a intentar).

Variable PP_SIMULAR_FALLO=1: inyecta duplicados para demostrar que el pipeline se detiene.
"""
import os
import sys

from pyspark.sql import Window
from pyspark.sql import functions as F

import config as C
from calidad import CalidadError, validar
from spark_utils import CAMPOS_PAYLOAD, get_spark, leer_raw_hdfs
from utils import (ahora, escribir_json, guardar_evidencia, hdfs_du_bytes, mb,
                   tamano_dir_local, ultimo_manifiesto)

RENOMBRAR = {":id": "socrata_id", ":created_at": "creado_fuente",
             ":updated_at": "actualizado_fuente"}
FECHAS = ["fechaexpedicion", "fechavencimiento", "fechaactivo", "fechainactivo"]
FECHAS_PASADAS = ["fechaexpedicion", "fechaactivo", "fechainactivo"]  # no pueden ser futuras
ENTEROS = ["expediente", "expedientecum", "consecutivocum"]
DECIMALES = ["cantidadcum", "cantidad"]
TEXTO_MAYUS = ["titular", "nombrerol", "principioactivo", "viaadministracion",
               "formafarmaceutica", "tiporol", "modalidad", "unidadmedida", "unidad",
               "atc", "descripcionatc", "unidadreferencia", "concentracion",
               "estadoregistro", "registrosanitario"]
TEXTO_LIBRE = ["producto", "descripcioncomercial"]
SIN_TILDES = ["titular", "nombrerol", "principioactivo", "viaadministracion",
              "formafarmaceutica"]
CATEGORIAS_NO_REPORTA = ["viaadministracion", "formafarmaceutica", "tiporol", "atc"]
GRUPOS_ATC = list("ABCDGHJLMNPRSV")


def limpiar_texto(c, mayus):
    x = F.regexp_replace(F.trim(F.col(c)), r"\s+", " ")
    if mayus:
        x = F.upper(x)
    return F.when(x == "", None).otherwise(x)


def parse_fecha(c):
    # Formato de la fuente: M/d/yyyy (texto). Se aceptan también formatos ISO.
    return F.coalesce(F.to_date(F.col(c), "M/d/yyyy"),
                      F.to_date(F.col(c), "yyyy-MM-dd'T'HH:mm:ss.SSS"),
                      F.to_date(F.col(c), "yyyy-MM-dd"))


def main():
    run_id = ahora().strftime("%Y%m%dT%H%M%S")
    spark = get_spark("fase5-zona-curada")
    pasos = []

    def paso(nombre, antes, despues, decision, extra=None):
        pasos.append({"paso": nombre, "filas_antes": antes, "filas_despues": despues,
                      "filas_eliminadas": antes - despues, "decision": decision,
                      **({"detalle": extra} if extra else {})})
        print(f"\n[{nombre}] {antes} -> {despues} filas ({antes - despues} eliminadas)")
        print(f"   Decisión: {decision}")
        if extra:
            print(f"   Detalle: {extra}")

    # ---------- 0. Lectura ----------
    raw = leer_raw_hdfs(spark)
    n_raw = raw.count()
    cols = [F.col("id_registro"), F.col("fecha_ingesta"), F.col("ts_ingesta")] + \
           [F.col("payload").getField(c).alias(RENOMBRAR.get(c, c)) for c in CAMPOS_PAYLOAD]
    df = raw.select(*cols)
    paso("0_lectura_raw_hdfs", n_raw, n_raw, "Se leen todos los sobres JSONL del data lake raw.")

    # ---------- 1. Filas sin columnas clave ----------
    cond_clave = ((F.coalesce(F.trim(F.col("id_registro")), F.lit("")) != "")
                  & F.col("expediente").isNotNull() & F.col("consecutivocum").isNotNull())
    sin_clave = df.filter(~cond_clave)
    df1 = df.filter(cond_clave)
    n1 = df1.count()
    paso("1_descartar_sin_clave", n_raw, n1,
         "Se descartan filas sin id_registro, expediente o consecutivocum: sin ellas no se "
         "puede identificar ni deduplicar el registro. Se guardan en la zona de descartados.")

    # ---------- 2. Duplicados por id_registro ----------
    w = Window.partitionBy("id_registro").orderBy(
        F.col("actualizado_fuente").desc_nulls_last(), F.col("ts_ingesta").desc())
    ranked = df1.withColumn("_rn", F.row_number().over(w))
    duplicados = ranked.filter("_rn > 1").drop("_rn")
    df2 = ranked.filter("_rn = 1").drop("_rn").cache()
    n2 = df2.count()
    llave_negocio = ["expedientecum", "consecutivocum", "principioactivo", "nombrerol", "tiporol"]
    n_llave = df2.select(*llave_negocio).distinct().count()
    paso("2_deduplicar_id_registro", n1, n2,
         "Se conserva una fila por id_registro (:id de Socrata): la versión con :updated_at "
         "más reciente. Los duplicados vienen de reingestas o republicaciones en Kafka.",
         f"Si se usara la llave de negocio {llave_negocio} quedarían {n_llave} filas: "
         f"{n2 - n_llave} registros legítimos se perderían. Por eso se usa :id.")

    # ---------- 3. Formatos de texto inconsistentes ----------
    distintos_cols = ["titular", "nombrerol", "principioactivo", "viaadministracion",
                      "formafarmaceutica"]
    antes_dist = df2.agg(*[F.countDistinct(c).alias(c) for c in distintos_cols]).first().asDict()
    df3 = df2
    for c in TEXTO_MAYUS:
        df3 = df3.withColumn(c, limpiar_texto(c, True))
    for c in TEXTO_LIBRE:
        df3 = df3.withColumn(c, limpiar_texto(c, False))
    for c in SIN_TILDES:
        df3 = df3.withColumn(c, F.translate(F.col(c), "ÁÉÍÓÚÜÀÈÌÒÙ", "AEIOUUAEIOU"))
    for c in ["titular", "nombrerol"]:
        df3 = df3.withColumn(c, F.regexp_replace(F.col(c), r"S\.\s*A\.\s*S\.?", "S.A.S."))
        df3 = df3.withColumn(c, F.regexp_replace(F.col(c), r"S\.\s+A\.", "S.A."))
        df3 = df3.withColumn(c, F.regexp_replace(F.col(c), r"([^ ])(S\.A\.S?\.?)$", "$1 $2"))
    df3 = (df3.withColumn("estadocum", F.initcap(F.trim(F.col("estadocum"))))
              .withColumn("es_muestra_medica",
                          F.upper(F.trim(F.col("muestramedica"))).isin("SI", "SÍ"))
              .drop("muestramedica"))
    despues_dist = df3.agg(*[F.countDistinct(c).alias(c) for c in distintos_cols]).first().asDict()
    paso("3_normalizar_texto", n2, n2,
         "trim, espacios múltiples -> uno, MAYÚSCULAS, sin tildes en categorías, "
         "'S. A.' -> 'S.A.', vacíos -> nulo, muestramedica -> booleano. No se eliminan filas.",
         {"valores_distintos_antes": antes_dist, "valores_distintos_despues": despues_dist})

    # ---------- 4. Fechas (texto -> date) ----------
    for c in FECHAS:
        df3 = df3.withColumn(f"{c}__txt", F.col(c)).withColumn(c, parse_fecha(c))
    for c in FECHAS_PASADAS:  # fechas futuras imposibles -> nulo
        df3 = df3.withColumn(f"{c}__futura", F.col(c) > F.current_date()) \
                 .withColumn(c, F.when(F.col(c) > F.current_date(), None).otherwise(F.col(c)))
    exprs = [F.sum((F.col(f"{c}__txt").isNotNull() & F.col(c).isNull()
                    & ~F.coalesce(F.col(f"{c}__futura"), F.lit(False)) if c in FECHAS_PASADAS
                    else F.col(f"{c}__txt").isNotNull() & F.col(c).isNull()).cast("int"))
             .alias(f"{c}_no_convertibles") for c in FECHAS]
    exprs += [F.sum(F.coalesce(F.col(f"{c}__futura"), F.lit(False)).cast("int"))
              .alias(f"{c}_futuras") for c in FECHAS_PASADAS]
    info_fechas = df3.agg(*exprs).first().asDict()
    df3 = df3.drop(*[f"{c}__txt" for c in FECHAS], *[f"{c}__futura" for c in FECHAS_PASADAS])
    paso("4_tipar_fechas", n2, n2,
         "Las fechas llegan como texto 'MM/DD/YYYY' (por eso min/max en la API salen mal). "
         "Se convierten a date; las no convertibles o futuras imposibles quedan nulas "
         "(no se borra la fila porque el resto del registro es válido).", info_fechas)

    # ---------- 5. Números (texto -> long/double) ----------
    for c in ENTEROS:
        df3 = df3.withColumn(f"{c}__txt", F.col(c)).withColumn(c, F.col(c).cast("long"))
    for c in DECIMALES:
        df3 = df3.withColumn(f"{c}__txt", F.col(c)).withColumn(c, F.col(c).cast("double"))
    df3 = df3.withColumn("cantidad__neg", F.col("cantidad") < 0) \
             .withColumn("cantidad", F.when(F.col("cantidad") < 0, None).otherwise(F.col("cantidad")))
    info_num = df3.agg(
        *[F.sum((F.col(f"{c}__txt").isNotNull() & F.col(c).isNull()).cast("int"))
          .alias(f"{c}_no_convertibles") for c in ENTEROS + DECIMALES],
        F.sum(F.coalesce(F.col("cantidad__neg"), F.lit(False)).cast("int")).alias("cantidad_negativa")
    ).first().asDict()
    df3 = df3.drop(*[f"{c}__txt" for c in ENTEROS + DECIMALES], "cantidad__neg")
    paso("5_tipar_numeros", n2, n2,
         "expediente, expedientecum y consecutivocum -> long; cantidad y cantidadcum -> double. "
         "Valores no numéricos o cantidades negativas -> nulo.", info_num)

    # ---------- 6. Nulos ----------
    revisar = ["atc", "viaadministracion", "formafarmaceutica", "tiporol", "titular",
               "fechaexpedicion", "fechainactivo", "cantidad", "principioactivo"]
    nulos = df3.agg(*[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in revisar]).first().asDict()
    for c in CATEGORIAS_NO_REPORTA:
        df3 = df3.withColumn(c, F.coalesce(F.col(c), F.lit("NO REPORTA")))
    paso("6_tratar_nulos", n2, n2,
         "Categorías nulas (atc, vía, forma, tipo de rol) -> 'NO REPORTA' para no perder filas en "
         "los conteos. fechainactivo nula SE CONSERVA: significa que el CUM sigue activo. "
         "Fechas y cantidades nulas se dejan nulas (no se inventan valores).",
         {"nulos_por_columna_antes_de_imputar": nulos})

    # ---------- 7. Columnas derivadas ----------
    # Un código ATC válido empieza por la letra del grupo y dos dígitos (ej. C09DX01)
    letra = F.substring(F.col("atc"), 1, 1)
    atc_valido = F.col("atc").rlike(r"^[ABCDGHJLMNPRSV][0-9]{2}")
    mapa = F.create_map(*[x for k, v in C.ATC_NIVEL1.items() for x in (F.lit(k), F.lit(v))])
    df_final = (df3
        .withColumn("atc_nivel1", F.when(atc_valido, letra).otherwise(F.lit("SIN_ATC")))
        .withColumn("grupo_atc", mapa[F.col("atc_nivel1")])
        .withColumn("cum_id", F.concat_ws("-", "expedientecum", "consecutivocum"))
        .withColumn("anio_expedicion", F.year("fechaexpedicion"))
        .withColumn("es_activo", F.col("estadocum") == "Activo")
        .withColumn("es_importador", F.col("tiporol") == "IMPORTADOR")
        .withColumn("fecha_proceso", F.current_timestamp()))
    paso("7_columnas_derivadas", n2, n2,
         "atc_nivel1 (primera letra ATC), grupo_atc, cum_id (expedientecum-consecutivocum), "
         "anio_expedicion, es_activo, es_importador.")

    if os.environ.get("PP_SIMULAR_FALLO") == "1":
        print("\n*** SIMULACIÓN DE FALLO: se inyectan 10 filas duplicadas ***")
        df_final = df_final.unionByName(df_final.limit(10))

    # ---------- Calidad (fase 6) ----------
    print("\n=== Validaciones de calidad ===")
    info = {"filas_raw": n_raw, "sin_clave": n_raw - n1, "duplicados": n1 - n2}
    try:
        reporte = validar(df_final, info, run_id)
    except CalidadError as e:
        print(f"\n PIPELINE DETENIDO: {e}")
        print("   No se escribió la zona curada y la marca de agua NO se movió.")
        escribir_json(os.path.join(C.EVIDENCIAS_DIR, "curada_pasos.json"), pasos)
        spark.stop()
        return 2

    # ---------- Escritura Parquet particionado ----------
    destino = f"{C.HDFS_URI}{C.HDFS_CURADA}"
    df_final.write.mode("overwrite").partitionBy("atc_nivel1").parquet(destino)
    (sin_clave.withColumn("motivo", F.lit("sin_columnas_clave"))
        .unionByName(duplicados.withColumn("motivo", F.lit("duplicado_id_registro")))
        .write.mode("overwrite").parquet(f"{C.HDFS_URI}{C.HDFS_DESCARTADOS}"))
    particiones = {r["atc_nivel1"]: r["count"] for r in
                   spark.read.parquet(destino).groupBy("atc_nivel1").count().collect()}
    print(f"\nZona curada escrita en {destino} (partitionBy atc_nivel1)")
    print("Filas por partición:", dict(sorted(particiones.items())))

    # ---------- Comparación de tamaño ----------
    tam = {
        "raw_jsonl_local_bytes": tamano_dir_local(C.RAW_DIR),
        "raw_jsonl_hdfs_bytes": hdfs_du_bytes(C.HDFS_RAW),
        "curada_parquet_hdfs_bytes": hdfs_du_bytes(C.HDFS_CURADA),
        "filas_raw_hdfs": n_raw,
        "filas_curadas": n2,
    }
    tam["reduccion_pct"] = round(100 * (1 - tam["curada_parquet_hdfs_bytes"]
                                        / max(tam["raw_jsonl_hdfs_bytes"], 1)), 1)
    print(f"Tamaño JSONL raw (HDFS): {mb(tam['raw_jsonl_hdfs_bytes'])} MB | "
          f"Parquet curado: {mb(tam['curada_parquet_hdfs_bytes'])} MB | "
          f"reducción {tam['reduccion_pct']} %")

    # ---------- Marca de agua: solo avanza si todo salió bien ----------
    man = ultimo_manifiesto()
    if man and man.get("total_registros", 0) > 0 and man.get("watermark_nuevo"):
        escribir_json(C.WATERMARK_FILE, {"watermark": man["watermark_nuevo"],
                                         "run_id_ingesta": man["run_id"],
                                         "confirmada": ahora().isoformat()})
        print(f"Marca de agua actualizada a {man['watermark_nuevo']}")

    guardar_evidencia("curada_pasos.json", pasos)
    guardar_evidencia("curada_tamanos.json", {**tam, "particiones": particiones,
                                              "calidad": reporte["resultado"]})
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
