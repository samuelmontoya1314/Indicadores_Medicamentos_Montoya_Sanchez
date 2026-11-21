"""Configuración central del pipeline CUM (INVIMA).

Todas las rutas y parámetros viven aquí para que cada script use exactamente
los mismos valores (fuente única de verdad).
"""
import os

# Carpeta raíz del proyecto (un nivel arriba de src/)
BASE_DIR = os.environ.get(
    "PP_BASE", os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)

# ---------------- Fuente ----------------
FUENTE = "datos.gov.co"
DATASET = "i7cb-raxc"
NOMBRE_DATASET = "Código Único de Medicamentos (CUM) vigentes - INVIMA"
API_URL = f"https://www.datos.gov.co/resource/{DATASET}.json"
META_URL = f"https://www.datos.gov.co/api/views/{DATASET}.json"
PAGE_SIZE = 50000          # máximo recomendado por Socrata por página
MAX_REINTENTOS = 5
TIMEOUT_S = 120
# Token opcional de Socrata (sube el límite de peticiones). No es obligatorio.
APP_TOKEN = os.environ.get("SOCRATA_APP_TOKEN")

# Campo que identifica cada registro sin repetirse (campo de sistema Socrata)
CAMPO_ID = ":id"
# Campo usado como marca de agua (última fecha de actualización del registro)
CAMPO_WATERMARK = ":updated_at"

# ---------------- Zonas locales ----------------
DATA_DIR = os.path.join(BASE_DIR, "data")
RAW_DIR = os.path.join(DATA_DIR, "raw", FUENTE, DATASET)
MANIFEST_DIR = os.path.join(DATA_DIR, "manifiestos")
STATE_DIR = os.path.join(DATA_DIR, "state")
WATERMARK_FILE = os.path.join(STATE_DIR, "watermark.json")
PUBLICADOS_FILE = os.path.join(STATE_DIR, "archivos_publicados.json")
CALIDAD_DIR = os.path.join(DATA_DIR, "calidad")
SERVICIO_DIR = os.path.join(DATA_DIR, "servicio")
SERVICE_DB = os.path.join(SERVICIO_DIR, "servicio.db")
EVIDENCIAS_DIR = os.path.join(BASE_DIR, "evidencias")
TMP_DIR = os.path.join(DATA_DIR, "tmp")

# ---------------- Kafka ----------------
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = "cum_medicamentos"
PARTICIONES = 3
GROUP_ID = "cum-hdfs"
LOTE_HDFS = 20000          # mensajes por archivo en HDFS (un archivo por lote)
IDLE_SEGUNDOS = 15         # el consumidor termina si no llegan mensajes en este tiempo

# ---------------- HDFS ----------------
HDFS_URI = os.environ.get("HDFS_URI", "hdfs://localhost:9000")
HDFS_RAW = f"/datalake/raw/{FUENTE}/{DATASET}"
HDFS_CURADA = "/datalake/curada/cum"
HDFS_DESCARTADOS = "/datalake/descartados/cum"
HDFS_MR_OUT = "/datalake/mapreduce/conteo_atc"
HDFS_CHECKPOINT = "/checkpoints/cum_stream"
HDFS_STREAM_OUT = "/datalake/stream/conteos_ventana"

# ---------------- Spark ----------------
SPARK_VERSION = "3.5.3"
KAFKA_PACKAGE = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{SPARK_VERSION}"

# Nombres de los grupos ATC nivel 1 (OMS)
ATC_NIVEL1 = {
    "A": "Tracto alimentario y metabolismo",
    "B": "Sangre y órganos hematopoyéticos",
    "C": "Sistema cardiovascular",
    "D": "Dermatológicos",
    "G": "Sistema genitourinario y hormonas sexuales",
    "H": "Hormonas sistémicas",
    "J": "Antiinfecciosos sistémicos",
    "L": "Antineoplásicos e inmunomoduladores",
    "M": "Sistema musculoesquelético",
    "N": "Sistema nervioso",
    "P": "Antiparasitarios",
    "R": "Sistema respiratorio",
    "S": "Órganos de los sentidos",
    "V": "Varios",
    "SIN_ATC": "Sin código ATC válido",
}

for _d in (RAW_DIR, MANIFEST_DIR, STATE_DIR, CALIDAD_DIR, SERVICIO_DIR,
           EVIDENCIAS_DIR, TMP_DIR):
    os.makedirs(_d, exist_ok=True)

