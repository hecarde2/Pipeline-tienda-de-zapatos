# `dashboard/` — panel web (Streamlit)

Aplicación Streamlit que **solo lee tablas Gold en Parquet** (nunca toca
PostgreSQL ni Spark), sirviendo el panel en el puerto 8501.

## Archivos

### `dashboard/lib.py`
Utilidades compartidas por todas las páginas:

- **Estilo** — CSS del diseño blanco hueso (fondo `#F6F3EC`, tarjetas
  blancas, acento cobre `#B45309`, sin emojis) y helpers de interfaz:
  `section()`, `kpi()`, `badge()`, `inicio_pagina()` (cabecera con badges y
  botón de actualización).
- **Datos** — `cargar()` con `@st.cache_data(ttl=REFRESH_SECONDS)` lee
  `lake/gold/<tabla>`; `_df()` avisa si falta una tabla (la página nunca se
  tumbar); `cargar_runs()` lee `lake/_metadata/runs.json`.
- **Filtros y export** — `filtro_fechas()` (selector de rango de fechas),
  `descarga_csv()` (botón de exportación), `fmt_eur()` y `tabla()`.
- **Refresco** — `con_refresco` envuelve el contenido de cada página en un
  `st.fragment(run_every=...)`.

### `dashboard/app.py` — página Inicio
KPIs globales (ingresos, unidades, días, producto estrella, stock crítico),
evolución de ventas con **filtro de fechas** y export CSV, top 5 de productos
y **tabla de últimas ejecuciones del batch** (estado, duración y filas por
capa, coloreada por `ok`/`error`).

### `dashboard/pages/` — páginas secundarias

| Archivo | Página | Contenido |
|---|---|---|
| `1_Ventas.py` | Ventas | Serie diaria y mensual con filtro de fechas + CSV, embudo de conversión |
| `2_Producto.py` | Producto | Top productos con filtro de marca/categoría, ingresos por categoría, tallas y colores, stock crítico y devoluciones (todos con CSV) |
| `3_Clientes.py` | Clientes | KPIs de clientes con filtro por segmento, gasto por segmento, ranking y carritos abandonados (CSV) |
| `4_Streaming.py` | Streaming | KPIs de eventos y alertas, actividad del stream y alertas de stock coloreadas por severidad (CSV) |

Streamlit genera el menú lateral automáticamente desde `pages/`. El menú es
**permanente**: se fuerza `initial_sidebar_state="expanded"`, en escritorio se
oculta el botón de contraer (CSS, ≥768 px) y un pequeño script limpia la
preferencia guardada del navegador por si el usuario lo había contraído.

## Variables de entorno

| Variable | Default | Efecto |
|---|---|---|
| `LAKE_ROOT` | `/data/lake` | Ruta del data lake |
| `AUTO_REFRESH` | `1` | Refresco automático on/off |
| `REFRESH_SECONDS` | `30` | Cadencia del refresco |

## Tema visual

`streamlit-config.toml` (en la raíz, copiado a `/app/.streamlit/config.toml`
en la imagen) configura los colores por defecto de Streamlit: fondo, texto,
bordes y paleta de los gráficos Vega-Lite.

## Comandos

```bash
# El panel ya corre con ./start; abrir en el navegador:
open http://localhost:8501

# Ver logs
docker compose logs -f dashboard

# Comprobación automática de errores de render (sin navegador)
docker compose --profile tools run --rm --no-deps -e AUTO_REFRESH=0 dashboard \
  python -c "from streamlit.testing.v1 import AppTest; \
             at=AppTest.from_file('dashboard/app.py'); at.run(); print(at.exception)"
```

(Repetir con cada página de `dashboard/pages/`.)
