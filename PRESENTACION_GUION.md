# Guion y Diapositivas de Sustentación (Máximo 8 Diapositivas · 10 min)

**Integrantes:** Martín Sánchez Urrego · Samuel Montoya Garzón  
**Curso:** Gestión de Datos Masivos G002 · Institución Universitaria Pascual Bravo · 2026-2  

> **Regla de la rúbrica (15 pts individuales):** Exponen todos los integrantes en 10 minutos + 5 minutos de preguntas. Cualquiera debe poder explicar cualquier línea de código y decisión del pipeline.
> - **Reparto sugerido (5 min cada uno):**
>   - **Martín Sánchez Urrego (Diapositivas 1 a 4 — 5 min):** Fuente, pregunta, arquitectura Lambda, elección de `id_registro` (`:id`), ingesta raw, Kafka (3 particiones, control de duplicados), HDFS por lotes y MapReduce vs. Spark.
>   - **Samuel Montoya Garzón (Diapositivas 5 a 8 — 5 min):** Zona curada en PySpark (decisiones de limpieza y partición Parquet `atc_nivel1`), calidad (7 reglas y parada automática), idempotencia con marca de agua `:updated_at`, Structured Streaming (ventana, watermark y checkpoint) y hallazgos del tablero en SQLite/Plotly.

---

## Diapositiva 1 — Pregunta, Fuente y Usuario (1 min)
- **Fuente:** Código Único de Medicamentos Vigentes (INVIMA) en `datos.gov.co` (`i7cb-raxc`), consultada por API Socrata.
- **Tamaño:** **156.281 registros** activos e inactivos de registros sanitarios vigentes, con 4 columnas de fecha (`fechaexpedicion`, `fechavencimiento`, `fechaactivo`, `fechainactivo`).
- **Pregunta principal:** *¿Cómo está compuesta y cómo ha evolucionado la oferta de medicamentos con registro vigente en Colombia (por grupo terapéutico ATC, vía de administración y origen fabricante vs. importador), y qué tanto depende el país de importaciones y de pocos titulares?*
- **Usuarios:** Ministerio de Salud e INVIMA (vigilancia de desabastecimiento y dependencia de importación), EPS/IPS (compras de medicamentos) y laboratorios.

---

## Diapositiva 2 — Arquitectura Lambda y Definición de `id_registro` (1.5 min)
- **¿Por qué `:id` de Socrata como `id_registro`?** El dataset viene desnormalizado (*CUM × principio activo × rol*). Probamos la llave compuesta de negocio (`expedientecum + consecutivocum + principioactivo + nombrerol + tiporol`) y descubrimos que se repite hasta 10 veces en registros reales (ej. expediente `20325287`). Usar esa llave borraría filas legítimas; por eso solicitamos explícitamente `$select=*,:id,:created_at,:updated_at` y usamos `:id` como `id_registro` y `:updated_at` como marca de agua.
- **Arquitectura Lambda:**
  - **Raw:** API Socrata (`requests`) → JSONL local con sobre + manifiesto → Apache Kafka (`cum_medicamentos`, 3 particiones) → Consumidor (`cum-hdfs`) → HDFS (`/datalake/raw`, 1 archivo por lote).
  - **Batch (Curada):** HDFS → MapReduce vs. Spark → PySpark (limpieza y calidad) → Parquet particionado por `atc_nivel1` → SQLite (`ind_*`).
  - **Speed Layer (Streaming):** Kafka → Spark Structured Streaming (ventana 30s, watermark 2m, checkpoint HDFS) → SQLite (`stream_conteos_ventana`).
  - **Servicio y Tablero:** vista `v_lambda_tiporol` en SQLite + tablero interactivo en Plotly HTML.

---

## Diapositiva 3 — Fase 2 y 3: Ingesta Raw y Mensajería con Kafka (1.5 min)
- **Ingesta (`01_ingesta.py`):**
  - Paginación de a 50.000 registros con `$order=:id` y hasta 5 reintentos con *exponential backoff* ante errores `429`/`5xx`.
  - Cada fila se guarda intacta dentro de `payload` en un sobre JSON (`fuente`, `dataset`, `id_registro`, `fecha_ingesta`, `run_id`, `payload`), particionado en carpetas `fecha_ingesta=AAAA-MM-DD`, más un manifiesto JSON con el conteo y el nuevo watermark candidato.
- **Kafka (`02_productor.py` y `03_consumidor_hdfs.py`):**
  - Tópico `cum_medicamentos` con **3 particiones** y `key = id_registro` (mismo registro → misma partición).
  - **¿Cómo evitamos duplicados si el productor corre dos veces?**
    1. `enable.idempotence=True` + `acks=all` en el productor.
    2. Registro de lotes enviados en `archivos_publicados.json` (una segunda ejecución omite el archivo).
    3. Deduplicación por `id_registro` en PySpark (capa curada) y UPSERT en SQLite.

---

## Diapositiva 4 — Fase 4: Data Lake en HDFS y MapReduce vs. Spark (1 min)
- **HDFS sin archivos pequeños:** el consumidor del grupo `cum-hdfs` acumula lotes de **20.000 mensajes** antes de escribir cada archivo en `/datalake/raw/datos.gov.co/i7cb-raxc/fecha_ingesta=AAAA-MM-DD/lote_*.jsonl` y solo hace `commit` del offset en Kafka después de confirmar la escritura en HDFS.
- **MapReduce (`mapper.py` / `reducer.py`) vs. Spark (`04_mapreduce.py`):**
  - Ambos cuentan los registros raw por grupo terapéutico ATC de nivel 1 (primera letra del código ATC: `A`..`V` o `SIN_ATC`).
  - El script compara automáticamente ambas salidas y deja la tabla de evidencia en `evidencias/mapreduce_vs_spark.txt` con coincidencia del 100 % en las 15 categorías y en el total (156.281 registros).

---

## Diapositiva 5 — Fase 5: Zona Curada en PySpark y Particionamiento Parquet (1.5 min)
- **Decisiones de limpieza documentadas con antes y después (`05_curada_spark.py`):**
  1. Descarte de filas sin llaves (`id_registro`, `expediente`, `consecutivocum`) → enviadas a `/datalake/descartados/cum`.
  2. Deduplicación por `id_registro` conservando el `:updated_at` más reciente.
  3. Normalización de texto (`UPPER`, sin tildes en categorías, `S. A.` → `S.A.`) que reduce razones sociales duplicadas por digitación.
  4. Conversión de fechas `MM/DD/YYYY` a `date` (anulando fechas futuras inválidas sin borrar el registro) y números a `long`/`double`.
  5. **Decisión clave sobre nulos:** `fechainactivo` nula (~118.168 filas) **no se borra ni se imputa**, porque representa precisamente que el CUM sigue activo.
- **Parquet particionado por `atc_nivel1`:** 15 particiones equilibradas que coinciden con los filtros analíticos del tablero. Compresión de ~85–90 % frente al JSONL raw.

---

## Diapositiva 6 — Fase 6: Calidad, Carga Incremental e Idempotencia (1.5 min · Demo en vivo)
- **7 validaciones automáticas (`calidad.py`):**
  - 3 críticas (`conteo > 0`, `sin nulos en llaves`, `id_registro único`): si alguna falla, lanza `CalidadError`, **detiene el pipeline**, no sobrescribe Parquet y **no avanza el watermark**.
  - 4 de advertencia (rangos de cantidades, rango de `fechaexpedicion`, dominio de `estadocum`, coherencia estado vs. `fechainactivo`).
- **Demostración de idempotencia (`run_pipeline.py`):**
  - **Corrida 1:** descarga 156.281 registros, pasa calidad y guarda `watermark = 2026-11-16T12:13:21.891Z`.
  - **Corrida 2 (en vivo):** consulta `$where=:updated_at > '2026-11-16T12:13:21.891'`, recibe 0 registros nuevos y los conteos de raw, curada y servicio permanecen idénticos.

---

## Diapositiva 7 — Fase 7: Structured Streaming (Capa Rápida) (1 min)
- **Flujo (`06_streaming.py`):** lee `cum_medicamentos` desde Kafka, agrupa por **ventana de 30 segundos** sobre el timestamp de Kafka y por `tiporol` (`FABRICANTE` / `IMPORTADOR`), con **`withWatermark("ts_kafka", "2 minutes")`** y **`checkpointLocation` en HDFS**.
- **Persistencia idempotente:** `foreachBatch` hace `INSERT ... ON CONFLICT(ventana_inicio, tiporol) DO UPDATE` en SQLite.
- **Evidencia de reinicio:** al volver a ejecutar `06_streaming.py`, lee el checkpoint en HDFS, procesa `0` mensajes nuevos y no duplica conteos (`evidencias/streaming_reinicio.json`).
- **Unión Lambda:** la vista `v_lambda_tiporol` cruza en SQLite los totales batch con los totales de streaming.

---

## Diapositiva 8 — Fase 8: Zona de Servicio, Tablero y Respuesta a la Pregunta (1 min)
- **Zona de servicio (`07_servicio.py`):** 7 tablas precalculadas en SQLite (`servicio.db`) para que el tablero no haga cálculos pesados.
- **Tablero (`08_tablero.html`) y hallazgos principales:**
  1. **Composición por ATC:** los grupos **Sistema nervioso (N)**, **Tracto alimentario y metabolismo (A)**, **Sistema cardiovascular (C)** y **Antiinfecciosos sistémicos (J)** concentran la mayor oferta de CUM activos.
  2. **Dependencia de importación:** grupos de alta complejidad como **Antineoplásicos e inmunomoduladores (L)** y **Hormonas/biológicos** presentan la mayor dependencia de importadores, mientras que en medicamentos orales comunes predomina la fabricación nacional (~73 % de las filas de rol son `FABRICANTE` y ~27 % `IMPORTADOR`).
  3. **Concentración por titular y vía:** la vía **ORAL** supera dos tercios de la oferta y los 10 mayores titulares concentran una fracción significativa de las presentaciones activas del país.

