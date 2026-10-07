# 👟 Pipeline de Datos — Tienda de Zapatos Online

Pipeline de datos completo sobre **arquitectura Medallion (Bronze → Silver → Gold)**
con **Apache Spark**, combinando **batch** (PostgreSQL + archivos históricos) y
**streaming** (Kafka), con un **dashboard en Streamlit** que consume las métricas Gold.

> Arranque en un solo comando: `./start`

---

## 📋 Índice

1. [Arquitectura](#-arquitectura)
2. [Tecnologías](#-tecnologías)
3. [Estructura del proyecto](#-estructura-del-proyecto)
4. [Documentación por carpetas](#-documentación-por-carpetas)
5. [Requisitos](#-requisitos)
6. [Arranque rápido](#-arranque-rápido)
7. [Qué hace cada servicio](#-qué-hace-cada-servicio)
8. [Las tres capas del lake](#-las-tres-capas-del-lake)
9. [Streaming y eventos](#-streaming-y-eventos)
10. [Dashboard](#-dashboard)
11. [Configuración (variables de entorno)](#-configuración-variables-de-entorno)
12. [Comandos útiles](#-comandos-útiles)
13. [Diseño y decisiones](#-diseño-y-decisiones)
14. [Problemas frecuentes](#-problemas-frecuentes)

---

## 📚 Documentación por carpetas

La carpeta [`docs/`](docs/) documenta qué hace cada carpeta y archivo del
proyecto:

| Documento | Cubre |
|---|---|
| [docs/README.md](docs/README.md) | Índice y mapa general |
| [docs/raiz.md](docs/raiz.md) | Raíz: `start`, `stop`, `Dockerfile`, `docker-compose.yml`, configs |
| [docs/app.md](docs/app.md) | `app/` — seed, bronze, silver, gold, batch, events, streaming |
| [docs/dashboard.md](docs/dashboard.md) | `dashboard/` — panel Streamlit y su diseño |
| [docs/db.md](docs/db.md) | `db/` — esquema de PostgreSQL |
| [docs/data.md](docs/data.md) | `data/` — archivos crudos y data lake |

---

## 🏗 Arquitectura

```
                         ┌──────────────────────────────┐
   PostgreSQL (OLTP) ───▶│  BRONZE  (crudo + metadata)  │◀─── CSV/JSON históricos (data/raw)
                         │  bronze/postgres/<tabla>/    │
                         │  bronze/historico_ventas/    │
                         └──────────────┬───────────────┘
                                        │ Spark (python -m app.batch)
                         ┌──────────────▼───────────────┐
                         │  SILVER  (limpio y validado) │──▶ silver/rechazados/<tabla>/
                         │  dedupe, estandarización,    │
                         │  tipos, joins consistentes   │
                         └──────────────┬───────────────┘
                                        │
                         ┌──────────────▼───────────────┐
                         │  GOLD    (métricas de negocio)│──▶ dashboard Streamlit
                         │  8 tablas de análisis batch   │
                         └──────────────▲───────────────┘
                                        │
   Navegadores ──▶ Kafka ──▶ Spark Structured Streaming (python -m app.streaming)
                (eventos-web)   ├──▶ bronze/eventos_web/stream/  (raw)
                                ├──▶ gold_actividad_stream
                                └──▶ gold_alertas  (stock bajo en vivo)
```

- **Batch**: `seed` → `bronze` → `silver` → `gold` (se ejecuta con `./start`).
- **Streaming**: Kafka → Structured Streaming → Bronze + Gold en vivo (servicio continuo).
- Ambos comparten `lake/bronze/eventos_web/`, así que el batch incluye en Silver
  tanto los eventos históricos como los que llegan en vivo.

---

## 🧰 Tecnologías

| Componente | Tecnología | Versión |
|---|---|---|
| Orquestación | Docker Compose | v2 |
| Lenguaje | Python | 3.11 |
| Procesamiento | Apache Spark (pyspark, local[2]) | 3.5.3 |
| Base transaccional | PostgreSQL | 16 |
| Mensajería | Apache Kafka (KRaft, single-node) | 3.7.2 |
| Productor de eventos | confluent-kafka | 2.16.0 |
| Dashboard | Streamlit | 1.65 |
| Almacenamiento | Parquet en FS local (rutas configurables a S3) | — |

---

## 📁 Estructura del proyecto

```
Pipeline-tienda-de-zapatos/
├── start / stop              # scripts de arranque y parada
├── docker-compose.yml        # servicios: postgres, kafka, events, streaming, dashboard (+tools: seed, batch)
├── Dockerfile                # imagen única con Spark + jars + Streamlit
├── entrypoint.sh             # resuelve JAVA_HOME
├── requirements.txt
├── app/
│   ├── config.py             # env, rutas del lake y reglas de negocio
│   ├── db.py                 # conexión psycopg2 con reintentos
│   ├── schemas.py            # EVENT_SCHEMA compartido
│   ├── spark_utils.py        # constructor de SparkSession (jars, heap, tz)
│   ├── seed.py               # genera datos de ejemplo (PG + CSV/JSON/JSONL)
│   ├── bronze.py             # PostgreSQL y archivos -> Bronze
│   ├── silver.py             # limpieza, validación, dedupe, rechazados
│   ├── gold.py               # 8 tablas de métricas de negocio
│   ├── batch.py              # orquestador Bronze -> Silver -> Gold
│   ├── events.py             # productor de eventos a Kafka (simulador web)
│   └── streaming.py          # Kafka -> Bronze + Gold (Structured Streaming)
├── dashboard/app.py          # panel Streamlit
├── db/init/01_schema.sql     # esquema transaccional (initdb)
└── data/                     # generado en runtime
    ├── raw/                  # archivos crudos (CSV, JSON, JSON Lines)
    └── lake/
        ├── bronze/           # crudo + metadatos de ingesta
        ├── silver/           # limpio (+ silver/rechazados/)
        ├── gold/             # métricas
        ├── _checkpoints/     # checkpoints de Spark Streaming
        └── _metadata/        # manifiestos
```

---

## ✅ Requisitos

- Docker ≥ 24 con **Docker Compose v2** (`docker compose version`)
- ~4 GB de RAM libres y ~3 GB de disco (imagen + jars + datos)
- Puertos libres: `5433` (PostgreSQL), `29092` (Kafka externo), `8501` (dashboard), `4040` (Spark UI)

---

## 🚀 Arranque rápido

```bash
./start
```

El script hace, en orden:

1. Crea `data/raw` y `data/lake`.
2. Construye la imagen (`docker compose build`).
3. Levanta `postgres` y `kafka` y espera a que estén *healthy*.
4. Ejecuta el **seed** si `data/raw` está vacío (datos de ejemplo).
5. Ejecuta el **batch**: Bronze → Silver → Gold.
6. Levanta en segundo plano `events` (productor Kafka), `streaming` y `dashboard`.

Al terminar imprime las URLs:

| Servicio | URL |
|---|---|
| Dashboard | http://localhost:8501 |
| Spark UI (streaming) | http://localhost:4040 |
| PostgreSQL | `localhost:5433` (db `tienda`, user `tienda`, pass `tienda123`) |
| Kafka (externo) | `localhost:29092` (topic `eventos-web`) |

Opciones:

```bash
./start --rebuild   # fuerza reconstrucción de la imagen
./start --reseed    # borra data/ y regenera todos los datos de ejemplo
./stop              # para servicios, conserva datos
./stop --volumes    # además borra el volumen de PostgreSQL
./stop --purge      # además borra data/raw y data/lake
```

---

## ⚙️ Qué hace cada servicio

| Servicio | Comando | Rol |
|---|---|---|
| `postgres` | postgres:16 | Base transaccional (esquema en `db/init/01_schema.sql`) |
| `kafka` | apache/kafka:3.7.2 | Broker KRaft single-node, topic `eventos-web` |
| `seed` *(tool)* | `python -m app.seed` | Genera datos: PG sucio a propósito + históricos CSV/JSON/JSONL |
| `batch` *(tool)* | `python -m app.batch` | Ingesta a Bronze, transforma a Silver y calcula Gold |
| `events` | `python -m app.events` | Productor de eventos de navegación a Kafka |
| `streaming` | `python -m app.streaming` | Spark Structured Streaming: Kafka → Bronze + Gold en vivo |
| `dashboard` | `streamlit run dashboard/app.py` | Panel web sobre las tablas Gold |

*(tool = profile `tools`: solo se ejecutan cuando tú lo pides).*

---

## 🥉 Las tres capas del lake

### Bronze — crudo con metadatos
- `bronze/postgres/<tabla>/_ingest_date=YYYY-MM-DD/` — Parquet añadiendo solo
  `_ingested_at`, `_source`, `_ingest_date`. Reescritura dinámica de particiones:
  reejecutable e idempotente sin borrar días anteriores.
- `bronze/historico_ventas/`, `bronze/historico_devoluciones/`, `bronze/proveedores/`
  — copias literales de los archivos crudos.
- `bronze/eventos_web/historico/` (JSON Lines) y `bronze/eventos_web/stream/`
  (micro-batches del streaming).

### Silver — limpio y validado (11 tablas)
`silver_clientes`, `silver_productos`, `silver_variantes`, `silver_inventario`,
`silver_pedidos`, `silver_pedido_detalle` (pedidos + detalle + pagos),
`silver_devoluciones`, `silver_historico_ventas`, `silver_historico_devoluciones`,
`silver_proveedores`, `silver_eventos_navegacion`.

Reglas aplicadas:

- **Deduplicación**: por clave de negocio (`event_id`, `id`, `pedido_id`…),
  conservando la última versión; el `order_placed` duplicado del histórico colapsa a uno.
- **Estandarización**: fechas heterogéneas (`YYYY-MM-DD`, `DD/MM/YYYY`…),
  precios en varios formatos (`89,90`, `1.234,56`, `€89 EUR`), tallas
  (`EU 36`, `T38`, `36,5` → `36`), nombres/ciudades en mayúsculas/minúsculas.
- **Validación**: correo con regex, `precio > 0`, categorías del catálogo,
  estados de pedido/pago conocidos, talla numérica válida.
- **Rechazados**: los inválidos se escriben en `silver/rechazados/<tabla>/`
  con `motivo_rechazo` (no se pierden, se apartan).
- **Consistencia**: `silver_pedido_detalle` une pedidos + detalle + pagos + variantes
  en una única vista de pedido.

### Gold — métricas para el negocio (8 tablas batch + 2 streaming)

| Tabla | Qué responde |
|---|---|
| `gold_ventas_diarias` | Ingresos y unidades por día (con año, mes, semana, fin de semana) |
| `gold_top_productos` | Zapatos más vendidos, con ranking global y por categoría |
| `gold_ventas_por_talla` | Tallas y colores con más demanda |
| `gold_embudo_conversion` | Vistas → carrito → checkout → compra, con tasas |
| `gold_carritos_abandonados` | Productos que se quedan en el carrito (sin `order_placed`) |
| `gold_stock_critico` | Variantes con stock ≤ umbral y valor de riesgo |
| `gold_tasa_devolucion` | Devoluciones por producto, % y motivo más frecuente |
| `gold_clientes_valor` | Frecuencia, gasto, ticket medio y segmento de clientes |
| `gold_actividad_stream` | Eventos y sesiones por tipo *(streaming)* |
| `gold_alertas` | Alertas `stock_low` con severidad *(streaming)* |

Ventas = pedidos en estados `confirmado/enviado/entregado` con pago confirmado
+ histórico de CSV, unificados con una columna `fuente`.

---

## 📡 Streaming y eventos

- **Productor (`events`)**: simula sesiones de navegación reales contra el catálogo
  de PostgreSQL (`product_viewed` → `search_performed` → `added_to_cart` →
  `checkout_started` → `order_placed` → `payment_*`), más alertas `stock_low`.
  Publica en el topic `eventos-web` con formato JSON idéntico al de los históricos.
- **Consumidor (`streaming`)**: Spark Structured Streaming con un único
  `foreachBatch` y checkpoint propio (`lake/_checkpoints/eventos_web/`) que:
  1. escribe el micro-batch crudo en `bronze/eventos_web/stream/`;
  2. agrega actividad por tipo → `gold_actividad_stream`;
  3. emite alertas de stock bajo → `gold_alertas` (severidad: bajo/crítico/agotado).
- Los duplicados (p. ej. el mismo `order_placed` reenviado) los resuelve **Silver**
  con dedupe por `event_id` en el siguiente batch.

Prueba manual del stream:

```bash
docker compose logs -f streaming        # ver micro-batches
docker compose --profile tools run --rm batch   # incorporar eventos a Silver/Gold
```

---

## 📊 Dashboard

`http://localhost:8501` — Streamlit leyendo solo de `lake/gold/` (Parquet → pandas):

- KPIs: ingresos, unidades, días con ventas, producto estrella, stock crítico.
- Serie diaria de ingresos y unidades + ingresos por mes.
- Top productos, ingresos por categoría, demanda por talla y color.
- Embudo de conversión con tasas y carritos abandonados por producto.
- Stock crítico, tasa de devolución por producto y clientes más valiosos.
- Actividad del streaming y alertas de stock en vivo.

Se actualiza solo cada 30 s (`AUTO_REFRESH=0` para desactivarlo,
`REFRESH_SECONDS` para cambiarlo) y tiene botón de actualización manual.

---

## 🔧 Configuración (variables de entorno)

Todas tienen valor por defecto; se pueden sobrescribir en `docker-compose.yml`:

| Variable | Default | Descripción |
|---|---|---|
| `DB_HOST/DB_PORT/DB_NAME/DB_USER/DB_PASSWORD` | `postgres/5432/tienda/tienda/tienda123` | PostgreSQL |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Bootstrap de Kafka |
| `KAFKA_TOPIC` | `eventos-web` | Topic de eventos |
| `KAFKA_STARTING_OFFSETS` | `latest` | Offset inicial del streaming |
| `LAKE_ROOT` | `/data/lake` | Raíz del data lake (local o `s3a://bucket/lake`) |
| `RAW_ROOT` | `/data/raw` | Raíz de archivos crudos |
| `STOCK_UMBRAL` | `5` | Umbral de stock crítico |
| `SPARK_DRIVER_MEMORY` | `768m` | Heap del driver Spark |
| `STREAM_TRIGGER` | `10 seconds` | Cadencia de micro-batches |
| `TZ` | `Europe/Madrid` | Zona horaria de Spark y contenedores |

**S3**: basta con `LAKE_ROOT=s3a://bucket/lake` (las jars `hadoop-aws` y
`aws-java-sdk-bundle` ya van en la imagen) y añadir credenciales AWS en el entorno.

---

## 🧪 Comandos útiles

```bash
# Todo detenido / todo arrancado
./stop && ./start

# Regenerar datos desde cero
./start --reseed

# Reejecutar el batch (idempotente)
docker compose --profile tools run --rm batch

# Solo Bronze, Silver o Gold
docker compose --profile tools run --rm batch python -m app.bronze
docker compose --profile tools run --rm batch python -m app.silver
docker compose --profile tools run --rm batch python -m app.gold

# Regenerar el seed
docker compose --profile tools run --rm seed --force

# Logs
docker compose logs -f streaming
docker compose logs -f events

# Inspeccionar el lake
ls data/lake/gold
docker compose --profile tools run --rm --entrypoint bash batch

# Productor manual con más intensidad
docker compose run --rm events --duracion 120 --espera 0.1

# Ver el topic
docker exec -it tienda-zapatos-kafka-1 \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic eventos-web
```

---

## 🧠 Diseño y decisiones

- **Arquitectura Medallion** con separación estricta: Silver jamás toca PostgreSQL
  directamente, todo pasa por Bronze (trazabilidad de ingesta incluida).
- **Spark local en contenedor** (`local[2]`, driver 768 MB): suficiente para el
  volumen de datos y evita el coste de un clúster standalone.
- **FS local como data lake** con rutas configurables: `LAKE_ROOT` apunta a S3
  sin cambiar una línea de código. No se usa MinIO (no era necesario).
- **Datos "sucios" a propósito** en el seed (correos inválidos, tallas en formato
  raro, fechas y precios heterogéneos, duplicados) para que Silver tenga trabajo
  real y los rechazados sean visibles.
- **Una única imagen** para todos los servicios: menos reproducibilidad rota,
  mismas jars para batch y streaming.
- **Un solo `foreachBatch`** en el streaming con checkpoint propio: Bronze y Gold
  se actualizan de forma consistente respecto al offset consumido.
- **El batch incluye los eventos del stream** (Bronze es la fuente única), así que
  embudo, carritos abandonados y alertas se consolidan con la misma lógica.
- **Reproducibilidad**: seeds fijos en el seed, `enable.idempotence` en el productor,
  reescritura dinámica de particiones en Bronze.

---

## ⚠️ Problemas frecuentes

| Síntoma | Causa / solución |
|---|---|
| `./start` falla en el paso 4 | PostgreSQL aún no sano: `docker compose ps` y reintenta |
| Dashboard: "Aún no hay datos Gold" | Faltó el batch: `docker compose --profile tools run --rm batch` |
| Puerto ocupado | Cambia el mapeo en `docker-compose.yml` (`8501`, `5433`, `29092`, `4040`) |
| Streaming no recoge eventos | `docker compose logs -f streaming` y revisa `kafka` healthy |
| Falta memoria | Baja `SPARK_DRIVER_MEMORY` o cierra otras cargas |
| Datos corruptos o wanting empezar de cero | `./stop --purge && ./start` |
| Cambios en `app/` no se aplican | Los volúmenes montan `./app`; reinicia el servicio afectado |

---

## 📚 Estructura de los datos de ejemplo

- **800 clientes** (correos inválidos cada 97, mayúsculas cada 53), **48 productos**
  en 3 categorías (`deportivo`, `formal`, `sandalia`), variantes por talla/color/SKU.
- **3.500 pedidos** en 540 días con pagos y algún pago fallido.
- **Histórico de ventas** (2023-2024) en CSV con formatos de fecha y precio
  heterogéneos y algún registro inválido a propósito.
- **Devoluciones** históricas con motivos.
- **Catálogo de proveedores** en JSON.
- **21 días de eventos de navegación** (22 archivos JSON Lines, ~11.500 eventos)
  con duplicados reales (el embudo y los carritos abandonados tienen datos desde
  el primer batch).
