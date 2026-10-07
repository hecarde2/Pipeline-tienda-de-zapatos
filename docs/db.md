# `db/` — esquema de la base transaccional

PostgreSQL 16 como sistema de origen (OLTP) de la tienda.

## Estructura

```
db/
└── init/
    └── 01_schema.sql      # se ejecuta automáticamente en el primer arranque
```

El directorio `db/init/` se monta en los contenedores como
`/docker-entrypoint-initdb.d:ro`: cualquier `*.sql` se ejecuta la primera vez
que se crea el volumen `pgdata` (posteriormente no se vuelve a ejecutar; para
resetear: `./stop --volumes`).

## Tablas definidas

| Tabla | Contenido |
|---|---|
| `clientes` | nombre, correo, ciudad, fecha de registro |
| `productos` | nombre, marca, categoría (`deportivo`/`formal`/`sandalia`) y precio |
| `variantes` | combinación producto + talla + color con `sku` único |
| `inventario` | stock disponible por variante y bodega |
| `pedidos` | cabecera de pedido: cliente, fecha, estado, total |
| `detalle_pedido` | líneas del pedido (variante, cantidad, precio) |
| `pagos` | un pago por pedido: método, estado, monto |
| `devoluciones` | una devolución por pedido: motivo y fecha |

Además hay índices sobre `pedidos.fecha`, `pedidos.cliente_id`,
`variantes.producto_id` y `detalle_pedido.variante_id`.

## Quién usa esta base

- **`app/seed.py`** la rellena con datos de ejemplo (incluyendo suciedad
  intencionada: correos inválidos, precio 0, tallas en formatos raros).
- **`app/bronze.py`** la lee por JDBC y la copia a Bronze.
- **`app/events.py`** consulta el catálogo y el stock para generar eventos
  coherentes con los `sku` reales.

Silver **nunca** accede a PostgreSQL: siempre pasa por Bronze.

## Acceso

| Parámetro | Valor |
|---|---|
| Host (dentro de compose) | `postgres:5432` |
| Host (desde la máquina) | `localhost:5433` |
| Base / usuario / contraseña | `tienda` / `tienda` / `tienda123` |

```bash
docker compose exec postgres psql -U tienda -d tienda -c "\dt"
```
