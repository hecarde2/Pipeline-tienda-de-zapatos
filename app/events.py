"""Productor de eventos de navegación a Kafka (simulador de tráfico web).

Genera sesiones coherentes con el catálogo de PostgreSQL (ver producto -> buscar
-> añadir al carrito -> checkout -> pedido) y alertas de stock_low, con el mismo
formato JSON que genera app.seed para los históricos (EVENT_SCHEMA).

Uso:  python -m app.events [--duracion 600] [--espera 0.4]
"""

import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer

from app import config
from app.db import fetch_all

SESSION: dict = {}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def _evento(event_type: str, **extra) -> dict:
    """Construye un evento con el mismo shape que app.seed (EVENT_SCHEMA)."""
    ev = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "ts": _now_iso(),
        "session_id": SESSION.get("id"),
        "user_id": SESSION.get("user_id"),
        "producto_id": SESSION.get("producto_id"),
        "variante_id": SESSION.get("variante_id"),
        "sku": SESSION.get("sku"),
        "cantidad": None,
        "monto": None,
        "search_query": None,
        "stock_actual": None,
        "motivo": None,
        "canal": SESSION.get("canal", "web"),
    }
    ev.update(extra)
    return ev


class Catalogo:
    """Catálogo real de la BD para que los eventos apunten a skus existentes."""

    def __init__(self):
        self.variantes = fetch_all(
            "SELECT v.id AS variante_id, v.producto_id, v.sku, p.marca, p.categoria "
            "FROM variantes v JOIN productos p ON p.id = v.producto_id"
        )
        if not self.variantes:
            raise RuntimeError("No hay variantes en la BD. Ejecuta: python -m app.seed")
        self.cliente_ids = [r["id"] for r in fetch_all("SELECT id FROM clientes")]
        self.bajos = fetch_all(
            "SELECT v.id AS variante_id, v.producto_id, v.sku, i.stock_disponible AS stock_actual "
            "FROM variantes v JOIN inventario i ON i.variante_id = v.id "
            "WHERE i.stock_disponible <= %s",
            (config.STOCK_UMBRAL,),
        )

    def nueva_sesion(self) -> None:
        v = random.choice(self.variantes)
        SESSION.clear()
        SESSION.update({
            "id": str(uuid.uuid4()),
            "user_id": random.choice(self.cliente_ids) if self.cliente_ids and random.random() < 0.7 else None,
            "canal": random.choices(["web", "app"], weights=[70, 30])[0],
            "variante_id": v["variante_id"],
            "producto_id": v["producto_id"],
            "sku": v["sku"],
            "marca": v["marca"],
            "categoria": v["categoria"],
        })


def flujo_sesion(con_pedido: bool) -> list[dict]:
    """Patrón de sesión de navegación (embudo realista)."""
    eventos = [_evento("product_viewed")]
    if random.random() < 0.4:
        eventos.append(_evento(
            "search_performed",
            search_query=random.choice(["zapatillas running", "botas cuero",
                                        "sneakers blancos", "talla 42"]),
        ))
    if random.random() < 0.45:
        cantidad = random.choices([1, 2], weights=[85, 15])[0]
        monto = round(random.uniform(49.9, 149.9) * cantidad, 2)
        eventos.append(_evento("added_to_cart", cantidad=cantidad, monto=monto))
        if random.random() < 0.55:
            eventos.append(_evento("checkout_started", monto=monto))
            # checkout sin order_placed = carrito abandonado
            if con_pedido:
                eventos.append(_evento("order_placed", cantidad=cantidad, monto=monto))
                pagado = random.random() < 0.92
                if pagado:
                    eventos.append(_evento("payment_confirmed", monto=monto))
                else:
                    eventos.append(_evento(
                        "payment_failed", monto=monto,
                        motivo=random.choice(["fondos_insuficientes", "tarjeta_rechazada"]),
                    ))
    return eventos


def evento_stock(cat: Catalogo) -> dict | None:
    if not cat.bajos:
        return None
    b = random.choice(cat.bajos)
    SESSION.clear()
    SESSION.update({"id": str(uuid.uuid4()), "canal": "web",
                    "variante_id": b["variante_id"], "producto_id": b["producto_id"],
                    "sku": b["sku"], "user_id": None})
    return _evento("stock_low", stock_actual=b["stock_actual"])


def main() -> int:
    parser = argparse.ArgumentParser(description="Productor de eventos web -> Kafka")
    parser.add_argument("--duracion", type=int, default=600,
                        help="Segundos que dura la simulación (default 600)")
    parser.add_argument("--espera", type=float, default=0.4,
                        help="Segundos entre sesiones (default 0.4)")
    args = parser.parse_args()

    producer = Producer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP,
        "linger.ms": 5,
        "enable.idempotence": True,
    })
    cat = Catalogo()

    entregados, errores = 0, 0

    def callback(err, _msg):
        nonlocal entregados, errores
        if err:
            errores += 1
        else:
            entregados += 1

    fin = time.time() + args.duracion
    sesiones = 0
    print(f"[events] Publicando en '{config.KAFKA_TOPIC}' ({config.KAFKA_BOOTSTRAP}) "
          f"durante {args.duracion}s")

    while time.time() < fin:
        cat.nueva_sesion()
        for ev in flujo_sesion(con_pedido=random.random() < 0.4):
            producer.produce(config.KAFKA_TOPIC, json.dumps(ev).encode(), callback=callback)
            producer.poll(0)
        if sesiones % 5 == 4:
            stock = evento_stock(cat)
            if stock:
                producer.produce(config.KAFKA_TOPIC, json.dumps(stock).encode(), callback=callback)
        sesiones += 1
        if sesiones % 20 == 0:
            producer.flush()
            print(f"[events] sesiones={sesiones} entregados={entregados} errores={errores}")
        time.sleep(args.espera)

    producer.flush()
    print(f"[events] Finalizado: {sesiones} sesiones, {entregados} mensajes OK, "
          f"{errores} con error")
    return 0 if errores == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
