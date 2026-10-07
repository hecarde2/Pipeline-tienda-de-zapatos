# `data/` — datos generados (no versionados)

Directorio creado en runtime y excluido del git (`.gitignore`). Se monta como
volumen en `/data` de todos los contenedores.

```
data/
├── raw/                   # ARCHIVOS CRUDOS DE ENTRADA
│   ├── historico_ventas/  #   ventas 2023-2024 en CSV (formatos heterogéneos)
│   ├── devoluciones_historicas.csv
│   ├── proveedores.json   #   catálogo de proveedores
│   └── eventos_web/       #   eventos históricos en JSON Lines (21 días)
│
└── lake/                  # DATA LAKE (arquitectura Medallion)
    ├── bronze/            #   crudo + metadatos de ingesta
    ├── silver/            #   limpio y validado (+ rechazados/)
    ├── gold/              #   métricas de negocio
    ├── _checkpoints/      #   checkpoints de Spark Streaming
    └── _metadata/         #   manifiestos de ingesta
```

## `data/raw/` — entrada

Lo escribe `app/seed.py` y lo leen `app/bronze.py` (copia a Bronze) y
`app/silver.py` (histórico de eventos, vía Bronze). Es el punto de partida del
modo batch: si no existe, `start` avisa y no ejecuta el pipeline.

Regenerar todo: `./start --reseed`.

## `data/lake/bronze/` — capaBronze (crudo)

- `postgres/<tabla>/_ingest_date=YYYY-MM-DD/` — Parquet de cada tabla con tres
  columnas extra: `_ingested_at`, `_source`, `_ingest_date`. Solo se reescribe
  la partición del día (idempotente).
- `historico_ventas/`, `historico_devoluciones/`, `proveedores/` — copias
  literales de `data/raw/`.
- `eventos_web/historico/` — eventos históricos copiados por el batch.
- `eventos_web/stream/` — micro-batches que escribe el streaming desde Kafka.
- `_manifiesto.json` — registro de la última ingesta de archivos.

## `data/lake/silver/` — capaSilver (limpio)

11 tablas Parquet con dedupe, estandarización y validación aplicadas
(ver [../docs/app.md](../docs/app.md)). Además:

```
silver/
├── silver_clientes, silver_productos, ...   # tablas válidas
└── rechazados/                              # registros inválidos
      ├── clientes/      (motivo: correo_inválido)
      ├── productos/     (motivo: precio_o_categoria_invalido)
      ├── variantes/     (motivo: talla_invalida)
      └── ...                              # con columna motivo_rechazo
```

Los rechazados **no se descartan**: se apartan con su motivo para poder
auditarlos.

## `data/lake/gold/` — capa Gold (métricas)

10 tablas Parquet listas para el dashboard:

- **8 de batch** (`gold_ventas_diarias`, `gold_top_productos`,
  `gold_ventas_por_talla`, `gold_embudo_conversion`,
  `gold_carritos_abandonados`, `gold_stock_critico`, `gold_tasa_devolucion`,
  `gold_clientes_valor`) — las escribe `app/batch.py` con reescritura
  completa.
- **2 de streaming** (`gold_actividad_stream`, `gold_alertas`) — las escribe
  `app/streaming.py` en modo append; el batch no las toca.

## `data/lake/_checkpoints/` — checkpoints

Estado de los streamings de Spark (`eventos_web/`): offsets consumidos y
ventanas. Borrarlo haría que el streaming releyera desde
`KAFKA_STARTING_OFFSETS`.

## `data/lake/_metadata/` — manifiestos

Metadatos de ingesta (por ejemplo, qué archivos históricos se copiaron y
cuándo).

## Tamaño y limpieza

```bash
du -sh data/                     # peso total
./stop --purge                   # borra data/ y volúmenes (arranque limpio)
./start --reseed                 # solo regenera los datos de ejemplo
```
