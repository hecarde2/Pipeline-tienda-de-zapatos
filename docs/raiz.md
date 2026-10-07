# Raíz del proyecto — archivos de configuración y orquestación

Archivos que viven en la raíz del repositorio y para qué sirve cada uno.

## Scripts de orquestación

### `start`
Punto de entrada único. Ejecuta en orden:

1. Crea los directorios `data/raw` y `data/lake`.
2. Construye la imagen Docker (`docker compose build`).
3. Levanta `postgres` y `kafka` y espera a que estén *healthy*.
4. Crea el topic de Kafka `eventos-web` si no existe.
5. Ejecuta el **seed** solo si `data/raw` está vacío.
6. Ejecuta el **batch** completo (Bronze → Silver → Gold).
7. Levanta en segundo plano `events`, `streaming` y `dashboard`.

Opciones: `--rebuild` (reconstruye la imagen), `--reseed` (borra `data/` y
regenera todo).

### `stop`
Para todos los servicios y red del proyecto.

- `./stop` — conserva PostgreSQL y el data lake.
- `./stop --volumes` — además borra el volumen de PostgreSQL.
- `./stop --purge` — además borra `data/raw` y `data/lake` (arranque limpio).

## Construcción de la imagen

### `Dockerfile`
Imagen única `tienda-zapatos-pipeline:latest` usada por **todos** los servicios:

- Base `python:3.11-slim-bookworm` + OpenJDK 17 + `procps` + `tini`.
- Instala `requirements.txt` (pyspark, streamlit, confluent-kafka, pandas…).
- Descarga las jars de Maven a `/opt/spark-jars`: conector Kafka de Spark,
  driver JDBC de PostgreSQL, `hadoop-aws` + SDK de AWS (por si `LAKE_ROOT`
  apunta a S3) y codecs (zstd, lz4, snappy).
- Copia `app/`, `dashboard/`, `db/`, el tema de Streamlit y `entrypoint.sh`.
- Corre como usuario **uid 1001** para que los archivos escritos en `./data`
  pertenezcan al usuario del host.

### `entrypoint.sh`
Entrada de todos los contenedores: si `JAVA_HOME` no está definido o no existe,
lo deduce del binario `java` y luego ejecuta el comando del servicio.

### `requirements.txt`
Dependencias con versión fija (reproducibilidad): `pyspark==3.5.3`,
`psycopg2-binary`, `Faker`, `confluent-kafka`, `pandas`, `pyarrow`,
`streamlit`.

### `streamlit-config.toml`
Tema visual del dashboard (blanco hueso `#F6F3EC`, acento cobre `#B45309`,
bordes y paleta de gráficos). Se copia a `/app/.streamlit/config.toml` en la
imagen.

## Orquestación de servicios

### `docker-compose.yml`
Define la red y los 7 servicios:

| Servicio | Perfil | Rol |
|---|---|---|
| `postgres` | principal | PostgreSQL 16, puerto externo **5433**, esquema desde `db/init/` |
| `kafka` | principal | Kafka 3.7.2 en modo KRaft (sin ZooKeeper), externo en **29092** |
| `events` | principal | Productor de eventos web a Kafka (reinicio automático) |
| `streaming` | principal | Spark Structured Streaming (Spark UI en **4040**) |
| `dashboard` | principal | Streamlit en **8501** |
| `seed` | `tools` | Generador de datos de ejemplo |
| `batch` | `tools` | Pipeline Bronze → Silver → Gold |

Todos los servicios Python comparten el ancla `x-app`: misma imagen, mismas
variables de entorno (credenciales, topic, rutas del lake, umbral de stock) y
los volúmenes `./data:/data`, `./app:/app/app`, `./dashboard:/app/dashboard`,
`./db:/app/db`.

## Configuración auxiliar

### `.gitignore`
Excluye `data/` (generado), `__pycache__`, logs, `derby.log` y `.env`.

### `.dockerignore`
Reduce el contexto de build: no envía `data/`, `.git` ni basura de Python.

### `README.md`
Documentación principal: arquitectura, arranque, comandos, configuración por
variables de entorno y solución de problemas.
