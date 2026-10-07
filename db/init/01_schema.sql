-- Esquema de la base transaccional (tienda de zapatos online)
-- Se carga automáticamente en el primer arranque de PostgreSQL.

CREATE TABLE IF NOT EXISTS clientes (
    id             SERIAL PRIMARY KEY,
    nombre         TEXT NOT NULL,
    correo         TEXT NOT NULL,
    ciudad         TEXT,
    fecha_registro DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS productos (
    id        SERIAL PRIMARY KEY,
    nombre    TEXT NOT NULL,
    marca     TEXT NOT NULL,
    categoria TEXT NOT NULL CHECK (categoria IN ('deportivo', 'formal', 'sandalia')),
    precio    NUMERIC(10, 2) NOT NULL
);

CREATE TABLE IF NOT EXISTS variantes (
    id          SERIAL PRIMARY KEY,
    producto_id INTEGER NOT NULL REFERENCES productos (id),
    talla       TEXT NOT NULL,
    color       TEXT NOT NULL,
    sku         TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS inventario (
    variante_id      INTEGER PRIMARY KEY REFERENCES variantes (id),
    stock_disponible INTEGER NOT NULL,
    bodega           TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS pedidos (
    id         SERIAL PRIMARY KEY,
    cliente_id INTEGER NOT NULL REFERENCES clientes (id),
    fecha      TIMESTAMP NOT NULL,
    estado     TEXT NOT NULL,
    total      NUMERIC(12, 2) NOT NULL
);

CREATE TABLE IF NOT EXISTS detalle_pedido (
    pedido_id   INTEGER NOT NULL REFERENCES pedidos (id),
    variante_id INTEGER NOT NULL REFERENCES variantes (id),
    cantidad    INTEGER NOT NULL,
    precio      NUMERIC(10, 2) NOT NULL,
    PRIMARY KEY (pedido_id, variante_id)
);

CREATE TABLE IF NOT EXISTS pagos (
    pedido_id INTEGER PRIMARY KEY REFERENCES pedidos (id),
    metodo    TEXT NOT NULL,
    estado    TEXT NOT NULL,
    monto     NUMERIC(12, 2) NOT NULL
);

CREATE TABLE IF NOT EXISTS devoluciones (
    pedido_id INTEGER PRIMARY KEY REFERENCES pedidos (id),
    motivo    TEXT NOT NULL,
    fecha     DATE NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_pedidos_fecha ON pedidos (fecha);
CREATE INDEX IF NOT EXISTS idx_pedidos_cliente ON pedidos (cliente_id);
CREATE INDEX IF NOT EXISTS idx_variantes_producto ON variantes (producto_id);
CREATE INDEX IF NOT EXISTS idx_detalle_variantes ON detalle_pedido (variante_id);
