# `app/` — código del pipeline

Módulos Python que implementan las tres capas del lake, el productor de
eventos y el consumo de Kafka. Todos se ejecutan con
`python -m app.<modulo>` dentro del contenedor.

## Módulos de configuración y utilidades

### `config.py`
Fuente única de configuración. Lee las variables de entorno y expone:

- Conexión a PostgreSQL (`DB_*`) y JDBC para Spark.
- Kafka (`KAFKA_BOOTSTRAP`, `KAFKA_TOPIC`, offsets iniciales).
- Rutas del lake: `bronze_path()`, `silver_path()`, `gold_path()`,
  `rejects_path()`, `checkpoint_path()`, `metadata_path()`.
- Reglas de negocio: categorías, estados de pedido/pago, estados que cuentan
  como venta, eventos conocidos, umbral de stock (`STOCK_UMBRAL`), regex de
  correo.

### `db.py`
Conexión a PostgreSQL con psycopg2 y reintentos (espera a que el contenedor
esté sano). Aporta `connect()` y `fetch_all(sql, params)` (filas como
diccionarios), usado por el seed y por el productor de eventos.

### `schemas.py`
`EVENT_SCHEMA`: StructType compartido entre el seed (históricos), el
productor Kafka y el streaming. Define los campos de un evento de navegación
(`event_id`, `event_type`, `ts`, `session_id`, `sku`, `cantidad`, `monto`…).

### `spark_utils.py`
Constructor `get_spark(app_name)` de la SparkSession: `local[2]`, heap
configurable (`SPARK_DRIVER_MEMORY`), jars de `/opt/spark-jars`, zona horaria,
particiones y **reescritura dinámica de particiones** (clave para que Bronze
sea idempotente).

## Datos de ejemplo

### `seed.py`
Genera todos los datos de entrada (ejecutable con `--force` para regenerar):

1. **PostgreSQL** con suciedad intencionada: 800 clientes (correos inválidos
   cada 97, mayúsculas cada 53), 48 productos (uno con precio 0), variantes
   con tallas en formatos raros (`EU 36`, ` T40 `), 3.500 pedidos con pagos y
   devoluciones. Los ids se leen de la BD (la secuencia puede no empezar en 1).
2. **CSV históricos** en `data/raw/historico_ventas/` (2023-2024) con fechas y
   precios en formatos heterogéneos.
3. **Devoluciones históricas** y **catálogo de proveedores** (JSON).
4. **21 días de eventos de navegación** en JSON Lines con duplicados reales
   (para probar el dedupe de Silver y que el embudo tenga datos el primer día).

## Capas del data lake

### `bronze.py`
Entrada cruda con metadatos:

- `ingest_postgres(spark)` — lee las 8 tablas por JDBC y las escribe en
  `bronze/postgres/<tabla>/_ingest_date=YYYY-MM-DD/` añadiendo solo
  `_ingested_at`, `_source` y `_ingest_date`. Con reescritura dinámica se
  reemplaza únicamente la partición del día (reprocesable).
- `ingest_historicos()` — copia literales de `data/raw/` a Bronze (CSV,
  JSON de proveedores, eventos históricos) y escribe un manifiesto.

### `silver.py`
Limpieza y validación (11 tablas). Helpers: `talla_estandar()` (normaliza
`EU 36`/` T40 `/`36,5` → `40`), `fecha_multi()` (varios formatos de fecha) y
`_precio()` (UDF pandas: `1.234,56` → `1234.56`). Por cada tabla:

- Deduplicación por clave de negocio (la última ingesta gana).
- Estandarización de textos, fechas, monedas y tallas.
- Filtros de validación; lo inválido va a `silver/rechazados/<tabla>/` con un
  `motivo_rechazo` (nunca se pierde, se aparta).
- `silver_pedido_detalle` une pedidos + detalle + pagos + variantes en la
  vista consistente de un pedido.
- `silver_eventos` lee Bronze (histórico + stream) y deduplica por `event_id`.

`run_silver(spark)` ejecuta todas y devuelve los conteos.

### `gold.py`
Métricas de negocio (8 tablas batch), listas para consumo:

| Tabla | Responde |
|---|---|
| `gold_ventas_diarias` | ingresos y unidades por día (año, mes, semana, fin de semana) |
| `gold_top_productos` | más vendidos con ranking global y por categoría |
| `gold_ventas_por_talla` | demanda por talla y por color |
| `gold_embudo_conversion` | vistas → carrito → checkout → compra con tasas |
| `gold_carritos_abandonados` | productos en carrito sin `order_placed` |
| `gold_stock_critico` | variantes bajo el umbral y su valor de riesgo |
| `gold_tasa_devolucion` | devoluciones por producto, % y motivo principal |
| `gold_clientes_valor` | pedidos, gasto, ticket medio y segmento de clientes |

Las ventas unifican lo transaccional (pagos confirmados en estados de venta)
con el histórico de CSV mediante una columna `fuente`.

## Orquestación, batch y streaming

### `batch.py`
Orquestador del pipeline batch: valida que exista `data/raw`, ingiere a
Bronze, transforma a Silver y calcula Gold. Es idempotente (se puede reejecutar
sin efectos secundarios). Se ejecuta con `python -m app.batch`.

### `events.py`
Simulador de tráfico web publicado a Kafka con `confluent-kafka`:

- Lee el catálogo real de PostgreSQL para que los `sku` existan.
- Genera sesiones con patrón de embudo (`product_viewed` → `search_performed`
  → `added_to_cart` → `checkout_started` → `order_placed` → `payment_*`),
  incluyendo checkouts abandonados.
- Emite `stock_low` para variantes bajo el umbral.
- Parámetros: `--duracion` (segundos) y `--espera` (segundos entre sesiones).

### `streaming.py`
Spark Structured Streaming sobre Kafka con un único `foreachBatch` y
checkpoint propio (`lake/_checkpoints/eventos_web/`):

1. Escribe cada micro-batch crudo en `bronze/eventos_web/stream/`.
2. Agrega eventos y sesiones por tipo → `gold_actividad_stream`.
3. Genera alertas `stock_low` con severidad (bajo / crítico / agotado) →
   `gold_alertas`.

Configurable con `STREAM_TRIGGER` (cadencia de micro-batches, por defecto
10 s) y `KAFKA_STARTING_OFFSETS`.
