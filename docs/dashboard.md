# `dashboard/` — panel web (Streamlit)

Aplicación Streamlit que **solo lee tablas Gold en Parquet** (nunca toca
PostgreSQL ni Spark), sirviendo el panel en el puerto 8501.

## Archivos

### `dashboard/app.py`
Único módulo del panel. Estructura:

1. **Configuración y estilo** — `st.set_page_config` y un bloque de CSS que
   define el diseño:
   - Fondo **blanco hueso** (`#F6F3EC`) y tarjetas blancas con borde suave.
   - Acento cobre (`#B45309`), verde azulado para lo positivo y rojo para
     alertas.
   - Tarjetas KPI con barra de acento superior, títulos de sección con barra
     lateral, botones oscuros que se vuelven cobre al pasar el ratón.
   - Sin emojis: iconografía basada en texto, bordes y color.
2. **Carga de datos** — `cargar()` con `@st.cache_data(ttl=REFRESH_SECONDS)`
   lee `lake/gold/<tabla>` con pandas/pyarrow; `_df()` avisa si una tabla
   aún no existe (el panel nunca se rompe).
3. **Render** — la función `panel()` decorada con
   `@st.fragment(run_every=...)` refresca automáticamente cada 30 s
   (`AUTO_REFRESH=0` lo desactiva; `REFRESH_SECONDS` cambia la cadencia).

## Secciones del panel

| Sección | Tablas Gold que usa |
|---|---|
| Cabecera + badges del pipeline | — |
| KPIs (ingresos, unidades, días, producto estrella, stock crítico) | `gold_ventas_diarias`, `gold_top_productos`, `gold_stock_critico` |
| Evolución de ventas e ingresos por mes | `gold_ventas_diarias` |
| Top productos y demanda por talla/color | `gold_top_productos`, `gold_ventas_por_talla` |
| Embudo de conversión y carritos abandonados | `gold_embudo_conversion`, `gold_carritos_abandonados` |
| Operaciones: stock, devoluciones, clientes | `gold_stock_critico`, `gold_tasa_devolucion`, `gold_clientes_valor` |
| Streaming en vivo: actividad y alertas | `gold_actividad_stream`, `gold_alertas` |

Las alertas se colorean por severidad (`agotado` en rojo, `crítico` en cobre).

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
