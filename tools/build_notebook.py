"""Genera notebooks/pipeline_cum_colab.ipynb a partir de los scripts de src/.

El notebook es autocontenido: cada script se escribe con %%writefile en la fase
que le corresponde y luego se ejecuta, así el código queda visible en el notebook
y es idéntico al del repositorio. Ejecutar:  python tools/build_notebook.py
"""
import base64
import json
import os

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def leer(rel):
    with open(os.path.join(RAIZ, rel), encoding="utf-8") as f:
        return f.read()


celdas = []


def md(texto):
    celdas.append({"cell_type": "markdown", "metadata": {}, "source": texto.strip("\n")})


def code(texto):
    celdas.append({"cell_type": "code", "metadata": {}, "execution_count": None,
                   "outputs": [], "source": texto.strip("\n")})


def wf(rel):
    code(f"%%writefile {rel}\n{leer(rel)}")


MERMAID = """flowchart LR
  A["API Socrata datos.gov.co<br/>Python requests"] --> B["ZONA RAW<br/>JSONL sobre por fecha_ingesta<br/>+ manifiesto"]
  B --> C["Apache Kafka<br/>topico cum_medicamentos<br/>3 particiones, key=id_registro"]
  C --> D["Consumidor grupo cum-hdfs<br/>HDFS /datalake/raw<br/>1 archivo por lote"]
  D --> E["MapReduce Python<br/>conteo por ATC vs Spark"]
  D --> F["PySpark<br/>ZONA CURADA Parquet<br/>partitionBy atc_nivel1"]
  F --> G["Calidad + marca de agua<br/>idempotencia"]
  G --> H["ZONA SERVICIO<br/>SQLite indicadores"]
  C --> I["CAPA RAPIDA<br/>Structured Streaming<br/>ventana + watermark + checkpoint"]
  I --> H
  H --> J["Tablero Plotly HTML"]"""
url_diag = "https://mermaid.ink/img/" + base64.urlsafe_b64encode(MERMAID.encode()).decode()

# ------------------------------------------------------------------ Portada
md(f"""
# Pipeline de datos masivos: medicamentos vigentes en Colombia (CUM - INVIMA)
**Gestión de Datos Masivos G002 · Institución Universitaria Pascual Bravo · 2026-2**
**Integrantes:** Martín Sánchez Urrego · Samuel Montoya Garzón

| Aspecto | Detalle |
|---|---|
| Fuente | [Código Único de Medicamentos vigentes (INVIMA)](https://www.datos.gov.co/resource/i7cb-raxc.json), datos.gov.co, API Socrata |
| Tamaño | ~156.000 registros, actualización frecuente (última: nov 2026) |
| Columnas de fecha | `fechaexpedicion`, `fechavencimiento`, `fechaactivo`, `fechainactivo` |
| **id_registro** | Campo de sistema Socrata **`:id`** (único por fila). La fuente no tiene llave natural: ni `expediente + consecutivocum + principioactivo + nombrerol + tiporol` es única |
| Marca de agua | Campo de sistema **`:updated_at`** (última actualización de cada registro) |
| Arquitectura | **Lambda**: batch (Spark → Parquet → SQLite) + capa rápida (Kafka → Structured Streaming) |

### Pregunta principal
> ¿Cómo está compuesta la oferta de medicamentos con registro vigente en Colombia (por grupo
> terapéutico ATC, vía de administración y origen fabricante o importador), y qué tanto depende de
> importaciones y de pocos titulares?

**¿Quién usaría los indicadores?** El Ministerio de Salud y los analistas de política farmacéutica
(riesgo de desabastecimiento por dependencia de importaciones), EPS e IPS (planeación de compras) y
cadenas de droguerías.

### Arquitectura por zonas
![diagrama]({url_diag})

**¿Por qué Lambda?** La fuente es un *snapshot* que se descarga por lotes desde la API: la vista batch
(deduplicada, limpia y validada) es la verdad. Además, cada registro viaja por Kafka y una capa rápida
con Structured Streaming da conteos en ventanas de tiempo a medida que llegan los mensajes. La zona de
servicio une ambas vistas (`v_lambda_tiporol`). Una arquitectura Kappa obligaría a reprocesar todo
desde el log de Kafka para corregir datos, y aquí la corrección (deduplicar, tipar) es más natural en batch.
""")

# ------------------------------------------------------------------ Setup
md("""
## 0. Preparación del entorno (Google Colab)
Se instalan Java 11, Kafka 3.7 (KRaft, sin ZooKeeper) y Hadoop 3.3.6 pseudo-distribuido, como en los
laboratorios. La primera vez tarda unos 5 minutos (descarga de Hadoop).
""")
code("""
import os
BASE = "/content/pp-cum-invima"
os.makedirs(f"{BASE}/src/mapreduce", exist_ok=True)
os.makedirs(f"{BASE}/evidencias", exist_ok=True)
os.chdir(BASE)
os.environ.update({
    "JAVA_HOME": "/usr/lib/jvm/java-11-openjdk-amd64",
    "HADOOP_HOME": "/opt/hadoop",
    "KAFKA_HOME": "/opt/kafka",
    "PP_BASE": BASE,
    "PYSPARK_PYTHON": "python3",
})
os.environ["PATH"] = ("/opt/hadoop/bin:/opt/kafka/bin:/usr/lib/jvm/java-11-openjdk-amd64/bin:"
                      + os.environ["PATH"])
print(os.getcwd())
""")
wf("requirements.txt")
wf("setup_colab.sh")
code("""
!bash setup_colab.sh
!pip install -q -r requirements.txt
""")
md("Módulos compartidos: configuración, utilidades y sesión de Spark con el esquema del sobre.")
wf("src/config.py")
wf("src/utils.py")
wf("src/spark_utils.py")

# ------------------------------------------------------------------ Fase 2
md("""
## Fase 2. Ingesta a la zona raw
- Descarga por la API Socrata con **paginación** (`$limit=50000` + `$offset`, `$order=:id` para que el
  orden sea estable) y **reintentos** con espera exponencial ante 429/5xx o caídas de red.
- Cada registro se guarda **intacto** en `payload` dentro de un **sobre** con `fuente`, `dataset`,
  `id_registro`, `fecha_ingesta`, `ts_ingesta` y `run_id`.
- Archivos JSON Lines particionados por `fecha_ingesta=AAAA-MM-DD`.
- Cada ejecución deja un **manifiesto** (URL, parámetros, páginas, conteo, marca de agua).
- **Incremental:** si existe `data/state/watermark.json`, solo se piden registros con `:updated_at` mayor.
  La marca de agua se confirma únicamente cuando la zona curada pasa la calidad (fase 5/6).
""")
wf("src/01_ingesta.py")
code("!python src/01_ingesta.py")
code("""
import json, glob
man = json.load(open(json.load(open("data/state/ultimo_manifiesto.json"))["ruta"]))
print(json.dumps({k: v for k, v in man.items() if k != "paginas"}, indent=2, ensure_ascii=False))
print("\\nEjemplo de sobre:")
with open(man["archivo"]) as f:
    print(json.dumps(json.loads(f.readline()), indent=2, ensure_ascii=False))
!find data/raw -name "*.jsonl" -exec ls -lh {} \\;
""")

# ------------------------------------------------------------------ Fase 3
md("""
## Fase 3. Mensajería con Kafka
- Tópico `cum_medicamentos` con **3 particiones**.
- El productor publica cada línea del raw con **key = id_registro**: el mismo registro siempre va a la
  misma partición.
- El consumidor pertenece al grupo **`cum-hdfs`** y confirma los offsets solo después de escribir en HDFS.

**¿Qué pasa si el productor se ejecuta dos veces?** Kafka no deduplica por key: guardaría el mensaje
dos veces (entrega *al menos una vez*). Lo evitamos en tres niveles:
1. `enable.idempotence=True` + `acks=all`: los reintentos internos del productor no generan duplicados.
2. Registro de archivos publicados (`data/state/archivos_publicados.json`): un archivo raw ya publicado
   se omite (lo demostramos abajo). Y con la marca de agua, la siguiente ingesta solo trae registros nuevos.
3. Si aun así llegan duplicados (por ejemplo, republicación forzada o caída del consumidor antes del commit),
   la zona curada **deduplica por `id_registro`**.
""")
wf("src/02_productor.py")
code("!python src/02_productor.py")
code("!kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic cum_medicamentos | tee evidencias/kafka_topico_describe.txt")
md("**Demostración:** si se ejecuta el productor otra vez, no se republica nada.")
code("!python src/02_productor.py | tee evidencias/kafka_productor_segunda_ejecucion.txt")

# ------------------------------------------------------------------ Fase 4
md("""
## Fase 4. Data lake en HDFS y MapReduce
El consumidor guarda los mensajes en HDFS con la estructura `fuente/dataset/fecha_ingesta=...`,
**un archivo por lote** (20.000 mensajes) y no uno por mensaje, para evitar el problema de los
archivos pequeños en el NameNode.
""")
wf("src/03_consumidor_hdfs.py")
code("!python src/03_consumidor_hdfs.py")
code("""
!kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group cum-hdfs | tee evidencias/kafka_grupo_consumidor.txt
!hdfs dfs -ls -R /datalake/raw | tee evidencias/hdfs_ls_raw.txt
""")
md("""
**MapReduce (Hadoop Streaming con mapper y reducer en Python):** número de registros raw por grupo ATC
nivel 1 (primera letra del código ATC). Después se calcula lo mismo con Spark y se comparan los resultados.
""")
wf("src/mapreduce/mapper.py")
wf("src/mapreduce/reducer.py")
wf("src/04_mapreduce.py")
code("!python src/04_mapreduce.py")

# ------------------------------------------------------------------ Fase 5 y 6
md("""
## Fase 5. Zona curada con PySpark
| Problema detectado | Decisión | Por qué |
|---|---|---|
| Filas sin `id_registro`, `expediente` o `consecutivocum` | Se descartan (van a `/datalake/descartados`) | Sin ellas no se puede identificar ni deduplicar el registro |
| Duplicados por `id_registro` (reingestas o republicaciones en Kafka) | `row_number` por `id_registro`, se queda la versión con `:updated_at` más reciente | Kafka y el consumidor son *al menos una vez*. Una llave de negocio borraría registros legítimos |
| Fechas como texto `MM/DD/YYYY` | `to_date` → `date`; las no convertibles o futuras imposibles quedan nulas | El texto ordena mal (min/max erróneos). No se borra la fila porque el resto del registro sirve |
| Números como texto | `cast` a `long` o `double`; las cantidades negativas quedan nulas | Permite sumar y validar rangos |
| Texto inconsistente (espacios, mayúsculas, tildes, `S. A.`) | `trim`, colapsar espacios, MAYÚSCULAS, quitar tildes en categorías, normalizar `S.A.` | Evita que un mismo titular cuente como varios |
| Nulos en categorías | `'NO REPORTA'`. `fechainactivo` nula **se conserva** | Una `fechainactivo` nula significa que el CUM sigue activo, no es un error |

**Partición:** `atc_nivel1` (unas 15 carpetas `atc_nivel1=A`, `atc_nivel1=C`, ...). Tiene baja cardinalidad
y es el filtro principal del tablero y de la pregunta. Descartamos `expediente` (9.500 valores, demasiados
archivos pequeños) y `fecha_ingesta` (casi todo cae en la misma fecha, no ayuda a filtrar).

## Fase 6. Calidad, carga incremental e idempotencia
Validaciones automáticas (✱ = crítica, detiene el pipeline): ✱ conteo > 0 · ✱ sin nulos en columnas
clave · ✱ `id_registro` sin repetidos · cantidades en rango · fecha de expedición en rango · dominio de
`estadocum` · coherencia estado/fecha. El reporte (`data/calidad/`) incluye los descartados. Si falla una
regla crítica, **no se escribe la zona curada ni se mueve la marca de agua**.
""")
wf("src/calidad.py")
wf("src/05_curada_spark.py")
code("!python src/05_curada_spark.py")
code("""
import pandas as pd, json
pasos = json.load(open("evidencias/curada_pasos.json"))
display(pd.DataFrame(pasos)[["paso", "filas_antes", "filas_despues", "filas_eliminadas", "decision"]])
for p in pasos:
    if "detalle" in p:
        print(p["paso"], "->", json.dumps(p["detalle"], ensure_ascii=False))
print("\\nTamaños:", json.dumps(json.load(open("evidencias/curada_tamanos.json")), indent=1, ensure_ascii=False))
rep = json.load(open("data/calidad/reporte_calidad_ultimo.json"))
display(pd.DataFrame(rep["reglas"]))
print("Descartados:", rep["descartados"])
!hdfs dfs -ls /datalake/curada/cum | tee evidencias/hdfs_ls_curada.txt
!hdfs dfs -du -h /datalake | tee evidencias/hdfs_du.txt
""")

# ------------------------------------------------------------------ Fase 7
md("""
## Fase 7. Streaming: capa rápida (Lambda)
Spark Structured Streaming lee el tópico de Kafka y cuenta mensajes por **ventana de 30 s** y tipo de rol,
con **watermark de 2 minutos** y **checkpoint en HDFS**. Cada micro-lote se guarda en SQLite con UPSERT.
Se usa `trigger(availableNow=True)`: procesa lo disponible y se detiene. **Al ejecutarlo otra vez
(reinicio), el checkpoint hace que lea 0 mensajes y los totales no cambian.**

**Unión con el batch (Lambda):** la vista `v_lambda_tiporol` de la zona de servicio compara los registros
de la vista batch (deduplicada) con los mensajes de la capa rápida. Si hubiera duplicados en Kafka, la capa
rápida los contaría y el batch los corregiría: así funciona Lambda, la capa rápida es inmediata pero
aproximada y el batch es exacto.
""")
wf("src/06_streaming.py")
code("!python src/06_streaming.py")
md("**Reinicio del flujo** (mismo checkpoint): no debe reprocesar nada.")
code("!python src/06_streaming.py | tee evidencias/streaming_reinicio.txt")

# ------------------------------------------------------------------ Fase 8
md("""
## Fase 8. Zona de servicio y tablero
Los indicadores se calculan en Spark y se guardan en SQLite (`data/servicio/servicio.db`). El tablero
solo los lee. El tablero es un HTML interactivo con Plotly que también se publica en GitHub Pages (`docs/index.html`).
""")
wf("src/07_servicio.py")
code("!python src/07_servicio.py")
code("""
import sqlite3, pandas as pd
con = sqlite3.connect("data/servicio/servicio.db")
for t in ["ind_resumen", "ind_atc_estado", "ind_origen_atc", "ind_top_titulares", "v_lambda_tiporol"]:
    print("==", t); display(pd.read_sql(f"SELECT * FROM {t}", con).head(15))
""")
wf("src/08_tablero.py")
code("!python src/08_tablero.py")
code("""
from IPython.display import IFrame, HTML, display
display(HTML(open("dashboard/tablero.html", encoding="utf-8").read()))
""")

# ------------------------------------------------------------------ Idempotencia
md("""
## Demostración de idempotencia (fase 6): el pipeline completo se corre dos veces
`run_pipeline.py` ejecuta las fases 2 a 8 en orden y guarda los conteos de cada corrida. En ambas
corridas la API devuelve 0 registros nuevos (marca de agua), Kafka no recibe mensajes y los conteos
de raw, curada, servicio y streaming quedan **idénticos**.
""")
wf("src/run_pipeline.py")
code("!python src/run_pipeline.py --etiqueta corrida_1")
code("!python src/run_pipeline.py --etiqueta corrida_2 | tee evidencias/idempotencia_salida.txt")
md("""
**Demostración: una validación crítica falla y el pipeline se detiene.** Se inyectan duplicados con
`PP_SIMULAR_FALLO=1`: la regla `id_registro_sin_repetidos` falla, el pipeline se detiene en la fase 5 y
no se escribe nada en la zona curada ni en la de servicio.
""")
code("!PP_SIMULAR_FALLO=1 python src/run_pipeline.py --etiqueta fallo_simulado --sin-mapreduce | tee evidencias/fallo_calidad_simulado.txt")
code("""
# El reporte de calidad registra la falla. Volvemos a correr el paso de curada para dejar el estado sano.
!python src/05_curada_spark.py > /dev/null && echo "Zona curada restaurada"
""")

# ------------------------------------------------------------------ Entrega
md("## Empaquetar evidencias y datos para el repositorio")
code("""
!mkdir -p data/muestra_raw
!head -n 1000 $(ls data/raw/datos.gov.co/i7cb-raxc/fecha_ingesta=*/*.jsonl | head -1) > data/muestra_raw/muestra_1000.jsonl
import sqlite3, pandas as pd, os
con = sqlite3.connect("data/servicio/servicio.db")
os.makedirs("data/servicio/csv", exist_ok=True)
for (t,) in con.execute("SELECT name FROM sqlite_master WHERE type IN ('table','view')"):
    pd.read_sql(f"SELECT * FROM {t}", con).to_csv(f"data/servicio/csv/{t}.csv", index=False)
!zip -qr /content/entrega_pp.zip evidencias data/manifiestos data/calidad data/servicio data/muestra_raw dashboard docs src setup_colab.sh requirements.txt
from google.colab import files
files.download("/content/entrega_pp.zip")
""")

nb = {"cells": celdas, "metadata": {
    "colab": {"provenance": [], "name": "pipeline_cum_colab.ipynb"},
    "kernelspec": {"name": "python3", "display_name": "Python 3"},
    "language_info": {"name": "python"}}, "nbformat": 4, "nbformat_minor": 0}
for c in nb["cells"]:   # formato estándar: lista de líneas
    c["source"] = c["source"].splitlines(keepends=True)
salida = os.path.join(RAIZ, "notebooks", "pipeline_cum_colab.ipynb")
os.makedirs(os.path.dirname(salida), exist_ok=True)
with open(salida, "w", encoding="utf-8") as f:
    json.dump(nb, f, ensure_ascii=False, indent=1)
print("Notebook generado:", salida, f"({len(celdas)} celdas)")

