"""Configuración central del pipeline (todo configurable por variables de entorno)."""

import os
from pathlib import Path

# --- PostgreSQL -------------------------------------------------------------
DB_HOST = os.environ.get("DB_HOST", "postgres")
DB_PORT = int(os.environ.get("DB_PORT", "5432"))
DB_NAME = os.environ.get("DB_NAME", "tienda")
DB_USER = os.environ.get("DB_USER", "tienda")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "tienda123")

JDBC_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
JDBC_PROPS = {
    "user": DB_USER,
    "password": DB_PASSWORD,
    "driver": "org.postgresql.Driver",
}

# --- Kafka ------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "eventos-web")
KAFKA_STARTING_OFFSETS = os.environ.get("KAFKA_STARTING_OFFSETS", "latest")

# --- Data Lake --------------------------------------------------------------
# Por defecto FS local; bastaría con LAKE_ROOT=s3a://bucket/lake (+ credenciales
# AWS_* en el entorno) para escribir directamente en S3.
LAKE_ROOT = Path(os.environ.get("LAKE_ROOT", "/data/lake"))
RAW_ROOT = Path(os.environ.get("RAW_ROOT", "/data/raw"))

# --- Reglas de negocio ------------------------------------------------------
STOCK_UMBRAL = int(os.environ.get("STOCK_UMBRAL", "5"))
EMAIL_REGEX = r"^[^@\s]+@[^@\s]+\.[a-zA-Z]{2,}$"

CATEGORIAS = ("deportivo", "formal", "sandalia")
ESTADOS_PEDIDO = ("pendiente", "confirmado", "enviado", "entregado", "cancelado")
ESTADOS_PAGO = ("pendiente", "confirmado", "fallido")
VENTA_ESTADOS = ("confirmado", "enviado", "entregado")

EVENTOS_CONOCIDOS = (
    "product_viewed",
    "search_performed",
    "added_to_cart",
    "checkout_started",
    "order_placed",
    "payment_confirmed",
    "payment_failed",
    "stock_low",
)

BRONZE_TABLES = (
    "clientes",
    "productos",
    "variantes",
    "inventario",
    "pedidos",
    "detalle_pedido",
    "pagos",
    "devoluciones",
)

SILVER_TABLES = (
    "silver_clientes",
    "silver_productos",
    "silver_variantes",
    "silver_pedidos",
    "silver_pedido_detalle",
    "silver_devoluciones",
    "silver_historico_ventas",
    "silver_historico_devoluciones",
    "silver_proveedores",
    "silver_eventos_navegacion",
)

GOLD_TABLES = (
    "gold_ventas_diarias",
    "gold_top_productos",
    "gold_ventas_por_talla",
    "gold_embudo_conversion",
    "gold_carritos_abandonados",
    "gold_stock_critico",
    "gold_tasa_devolucion",
    "gold_clientes_valor",
    "gold_alertas",
    "gold_actividad_stream",
)


# --- Rutas del lake ---------------------------------------------------------
def bronze_path(*parts) -> Path:
    return LAKE_ROOT / "bronze" / Path(*parts)


def silver_path(*parts) -> Path:
    return LAKE_ROOT / "silver" / Path(*parts)


def gold_path(*parts) -> Path:
    return LAKE_ROOT / "gold" / Path(*parts)


def rejects_path(*parts) -> Path:
    return LAKE_ROOT / "silver" / "rechazados" / Path(*parts)


def checkpoint_path(*parts) -> Path:
    return LAKE_ROOT / "_checkpoints" / Path(*parts)


def metadata_path(*parts) -> Path:
    return LAKE_ROOT / "_metadata" / Path(*parts)


def spath(path) -> str:
    """Convierte Path a str para las APIs de Spark/Hadoop."""
    return str(path)
