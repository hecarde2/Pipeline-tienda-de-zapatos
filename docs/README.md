# Documentación del proyecto

Esta carpeta documenta **qué hace cada carpeta y archivo** del pipeline de la
tienda de zapatos online.

## Índice

| Documento | Carpeta que cubre |
|---|---|
| [raiz.md](raiz.md) | Raíz del repo: `start`, `stop`, `Dockerfile`, `docker-compose.yml`, `requirements.txt`, `entrypoint.sh`, configuraciones |
| [app.md](app.md) | `app/` — todo el código Python del pipeline (seed, bronze, silver, gold, batch, events, streaming) |
| [dashboard.md](dashboard.md) | `dashboard/` — panel web en Streamlit |
| [db.md](db.md) | `db/` — esquema de la base transaccional PostgreSQL |
| [data.md](data.md) | `data/` — archivos generados: crudos (`raw/`) y data lake (`lake/`) |

## Mapa rápido

```
Pipeline-tienda-de-zapatos/
│
├── start, stop            →  orquestación local (ver raiz.md)
├── Dockerfile             →  imagen única con Spark + jars + Streamlit
├── docker-compose.yml     →  8 servicios (ver raiz.md)
├── requirements.txt       →  dependencias Python pinneadas
├── entrypoint.sh          →  resuelve JAVA_HOME antes de arrancar
├── streamlit-config.toml  →  tema visual del dashboard (blanco hueso)
│
├── app/                   →  ELT batch + API + streaming (ver app.md)
├── dashboard/             →  métricas Gold en Streamlit (ver dashboard.md)
├── db/                    →  esquema PostgreSQL (ver db.md)
├── data/                  →  generado en runtime (ver data.md)
│     ├── raw/             →  archivos crudos de entrada
│     └── lake/            →  bronze / silver / gold
└── docs/                  →  esta documentación
```

## Flujo de datos en una frase

`app/seed.py` genera datos de ejemplo en PostgreSQL y `data/raw/` →
`app/batch.py` (que además consume la API externa con `app/api.py`) los ingiere
a **Bronze**, los limpia en **Silver** y calcula las métricas de **Gold** →
`dashboard/app.py` las muestra; en paralelo, `app/events.py` publica eventos a
Kafka y `app/streaming.py` los consume para alimentar Bronze y las tablas Gold
en vivo.
