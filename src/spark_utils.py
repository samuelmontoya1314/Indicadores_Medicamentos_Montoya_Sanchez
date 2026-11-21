"""Sesión de Spark y esquema del «sobre» raw, compartidos por los scripts Spark."""
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

import config as C

# Campos del registro original (payload). Se leen todos como texto: la zona raw
# no corrige tipos; eso se hace en la zona curada.
CAMPOS_PAYLOAD = [
    ":id", ":created_at", ":updated_at",
    "expediente", "producto", "titular", "registrosanitario", "fechaexpedicion",
    "fechavencimiento", "estadoregistro", "expedientecum", "consecutivocum",
    "cantidadcum", "descripcioncomercial", "estadocum", "fechaactivo",
    "fechainactivo", "muestramedica", "unidad", "atc", "descripcionatc",
    "viaadministracion", "concentracion", "principioactivo", "unidadmedida",
    "cantidad", "unidadreferencia", "formafarmaceutica", "nombrerol", "tiporol",
    "modalidad", "ium",
]

ESQUEMA_PAYLOAD = StructType([StructField(c, StringType()) for c in CAMPOS_PAYLOAD])

ESQUEMA_SOBRE = StructType([
    StructField("fuente", StringType()),
    StructField("dataset", StringType()),
    StructField("id_registro", StringType()),
    StructField("fecha_ingesta", StringType()),
    StructField("ts_ingesta", StringType()),
    StructField("run_id", StringType()),
    StructField("payload", ESQUEMA_PAYLOAD),
])


def get_spark(app, kafka=False):
    b = (SparkSession.builder.appName(app).master("local[*]")
         .config("spark.driver.memory", "4g")
         .config("spark.sql.shuffle.partitions", "8")
         .config("spark.hadoop.fs.defaultFS", C.HDFS_URI)
         .config("spark.sql.session.timeZone", "America/Bogota")
         .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
         .config("spark.sql.ansi.enabled", "false")
         .config("spark.ui.showConsoleProgress", "false"))
    if kafka:
        b = b.config("spark.jars.packages", C.KAFKA_PACKAGE)
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def leer_raw_hdfs(spark):
    """Lee todos los sobres JSONL del data lake raw en HDFS."""
    return spark.read.schema(ESQUEMA_SOBRE).json(f"{C.HDFS_URI}{C.HDFS_RAW}")

