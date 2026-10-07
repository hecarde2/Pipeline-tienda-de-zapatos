"""Genera los datos de ejemplo del proyecto.

Dos salidas:
  1. Base de datos PostgreSQL (clientes, productos, variantes, inventario,
     pedidos, detalle_pedido, pagos, devoluciones) con algo de "suciedad"
     intencionada (correos inválidos, tallas en formato raro...) para que
     Silver tenga trabajo de limpieza.
  2. Archivos históricos crudos en RAW_ROOT: ventas de años anteriores en CSV
     con formatos heterogéneos, devoluciones, catálogo de proveedores en JSON
     y eventos de navegación históricos en JSON Lines (para que el embudo y
     los carritos abandonados tengan datos desde el primer batch).

Uso:  python -m app.seed [--force]
"""

import csv
import json
import random
import sys
import uuid
from datetime import datetime, timedelta
from pathlib import Path

from faker import Faker

from app import config
from app.db import connect

SEED = 42
fake = Faker("es_ES")
Faker.seed(SEED)
random.seed(SEED)

MARCAS = [
    "Nike", "Adidas", "Puma", "New Balance", "Clarks", "Geox",
    "Munich", "Callao", "Mustang", "Hush Puppies",
]
MODELOS = {
    "deportivo": ["Runner Air", "Trail Grip", "Court Flex", "Sprint Pro", "Gym Pulse"],
    "formal": ["Oxford Classic", "Derby Executive", "Loafer Milano", "Monk Strap", "Chelsea Boot"],
    "sandalia": ["Summer Slide", "Beach Wrap", "Roma Sandal", "Ankle Tie", "Pool Flip"],
}
TALLAS_BASE = [36, 37, 38, 39, 40, 41, 42, 43]
COLORES = ["negro", "marrón", "blanco", "azul marino", "beige", "rojo"]
CIUDADES = ["Madrid", "Barcelona", "Valencia", "Sevilla", "Bilbao", "Zaragoza", "Málaga", "Granada"]
BODEGAS = ["BOD-MAD", "BOD-BCN", "BOD-VAL"]
MOTIVOS_DEVOLUCION = ["talla pequeña", "talla grande", "defecto de fabricación", "no era lo esperado", "llegó dañado"]
METODOS_PAGO = ["tarjeta", "paypal", "bizum", "transferencia"]
CANALES_HIST = ["web", "app", "marketplace"]

N_CLIENTES = 800
N_PRODUCTOS = 48
N_PEDIDOS = 3500
DIAS_PEDIDOS = 540


# ---------------------------------------------------------------------------
# PostgreSQL
# ---------------------------------------------------------------------------

def seed_postgres(force: bool = False) -> None:
    conn = connect()
    conn.autocommit = False
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM productos")
    existing = cur.fetchone()[0]
    if existing and not force:
        print(f"[seed] PostgreSQL ya poblado ({existing} productos) — usa --force para regenerar.")
        cur.close()
        conn.close()
        return
    if existing and force:
        for tabla in ("devoluciones", "pagos", "detalle_pedido", "pedidos",
                      "inventario", "variantes", "productos", "clientes"):
            cur.execute(f"TRUNCATE TABLE {tabla} RESTART IDENTITY CASCADE")

    hoy = datetime.now()

    # --- clientes -----------------------------------------------------------
    clientes = []
    for i in range(1, N_CLIENTES + 1):
        nombre = fake.name()
        ciudad = random.choice(CIUDADES)
        registro = hoy - timedelta(days=random.randint(1, 1000))
        if i % 97 == 0:
            correo = f"cliente{i}sin-arroba"          # inválido -> silver lo rechaza
        elif i % 53 == 0:
            correo = f"  Cliente{i}@Example.COM  "     # válido tras limpiar
        else:
            correo = fake.email()
        clientes.append((nombre, correo.strip(), ciudad, registro.date()))
    cur.executemany(
        "INSERT INTO clientes (nombre, correo, ciudad, fecha_registro) VALUES (%s,%s,%s,%s)",
        clientes,
    )
    # Los ids reales los asigna la secuencia (puede no empezar en 1 si hubo
    # intentos previos): se leen de la BD en vez de asumir 1..N.
    cur.execute("SELECT id FROM clientes ORDER BY id")
    cliente_ids = [r[0] for r in cur.fetchall()]

    # --- productos ----------------------------------------------------------
    productos = []
    for pid in range(1, N_PRODUCTOS + 1):
        categoria = random.choice(list(MODELOS))
        marca = random.choice(MARCAS)
        modelo = random.choice(MODELOS[categoria])
        nombre = f"{marca} {modelo} {pid}"
        precio = round(random.uniform(29.9, 189.9), 2)
        if pid % 41 == 0:
            precio = 0.0  # basura intencionada -> silver rechaza
        productos.append((nombre, marca, categoria, precio))
    cur.executemany(
        "INSERT INTO productos (nombre, marca, categoria, precio) VALUES (%s,%s,%s,%s)",
        productos,
    )
    cur.execute("SELECT id FROM productos ORDER BY id")
    producto_ids = [r[0] for r in cur.fetchall()]

    # --- variantes + inventario --------------------------------------------
    variantes = []
    inventario = []
    for pid in range(1, N_PRODUCTOS + 1):
        n_tallas = random.randint(4, 6)
        tallas = sorted(random.sample(TALLAS_BASE, n_tallas))
        for talla in tallas:
            for color in random.sample(COLORES, random.choice([2, 3])):
                if random.random() < 0.08:
                    talla_txt = random.choice([f"EU {talla}", f"{talla},5" if random.random() < 0.5 else f" T{talla} "])
                else:
                    talla_txt = str(talla)
                sku = f"ZAP-{pid:03d}-{talla}-{color[:3].upper()}-{random.randint(10, 99)}"
                variantes.append((producto_ids[pid - 1], talla_txt, color, sku))
    cur.executemany(
        "INSERT INTO variantes (producto_id, talla, color, sku) VALUES (%s,%s,%s,%s)",
        variantes,
    )
    cur.execute("SELECT id, producto_id FROM variantes ORDER BY id")
    var_a_producto = {r[0]: r[1] for r in cur.fetchall()}
    variant_ids = list(var_a_producto)
    for vid in variant_ids:
        if random.random() < 0.04:
            stock = random.randint(0, 3)   # stock bajo -> gold_stock_critico
        else:
            stock = random.randint(10, 120)
        inventario.append((vid, stock, random.choice(BODEGAS)))
    cur.executemany(
        "INSERT INTO inventario (variante_id, stock_disponible, bodega) VALUES (%s,%s,%s)",
        inventario,
    )

    # --- precios válidos en memoria para pedidos ---------------------------
    cur.execute("SELECT id, precio FROM productos")
    precios = {pid: float(precio) for pid, precio in cur.fetchall()}

    # --- pedidos ------------------------------------------------------------
    pedidos, detalle, pagos, devoluciones = [], [], [], []
    for oid in range(1, N_PEDIDOS + 1):
        cliente_id = random.choice(cliente_ids)
        dias_atras = int(random.triangular(0, DIAS_PEDIDOS, DIAS_PEDIDOS * 0.35))
        fecha = hoy - timedelta(days=dias_atras, hours=random.randint(0, 23),
                                minutes=random.randint(0, 59))
        estado = random.choices(
            ["confirmado", "enviado", "entregado", "cancelado", "pendiente"],
            weights=[20, 25, 40, 8, 7],
        )[0]
        n_lineas = random.choices([1, 2, 3, 4], weights=[55, 28, 12, 5])[0]
        vids = random.sample(variant_ids, n_lineas)
        total = 0.0
        for vid in vids:
            prod_id = var_a_producto[vid]
            precio = precios[prod_id]
            if precio <= 0:
                precio = 49.9
            cantidad = random.choices([1, 2, 3], weights=[80, 16, 4])[0]
            detalle.append((oid, vid, cantidad, precio))
            total += precio * cantidad
        total = round(total, 2)
        pedidos.append((cliente_id, fecha, estado, total))

        metodo = random.choice(METODOS_PAGO)
        if estado == "cancelado":
            estado_pago = random.choice(["fallido", "pendiente"])
        elif estado == "pendiente":
            estado_pago = "pendiente"
        else:
            estado_pago = random.choices(["confirmado", "fallido"], weights=[97, 3])[0]
        pagos.append((oid, metodo, estado_pago, total))

        if estado == "entregado" and random.random() < 0.08:
            dev_fecha = fecha.date() + timedelta(days=random.randint(5, 30))
            if dev_fecha <= hoy.date():
                devoluciones.append((oid, random.choice(MOTIVOS_DEVOLUCION), dev_fecha))

    cur.executemany(
        "INSERT INTO pedidos (cliente_id, fecha, estado, total) VALUES (%s,%s,%s,%s)",
        pedidos,
    )
    # Mapea el oid correlativo (1..N) al id real asignado por la secuencia.
    cur.execute("SELECT id FROM pedidos ORDER BY id")
    pedido_ids = [r[0] for r in cur.fetchall()]
    detalle = [(pedido_ids[oid - 1], vid, cant, prec) for oid, vid, cant, prec in detalle]
    pagos = [(pedido_ids[oid - 1], metodo, est, monto) for oid, metodo, est, monto in pagos]
    devoluciones = [(pedido_ids[oid - 1], motivo, fecha) for oid, motivo, fecha in devoluciones]
    cur.executemany(
        "INSERT INTO detalle_pedido (pedido_id, variante_id, cantidad, precio) VALUES (%s,%s,%s,%s)",
        detalle,
    )
    cur.executemany(
        "INSERT INTO pagos (pedido_id, metodo, estado, monto) VALUES (%s,%s,%s,%s)",
        pagos,
    )
    cur.executemany(
        "INSERT INTO devoluciones (pedido_id, motivo, fecha) VALUES (%s,%s,%s)",
        devoluciones,
    )
    conn.commit()
    print(f"[seed] PostgreSQL: {len(clientes)} clientes, {len(productos)} productos, "
          f"{len(variantes)} variantes, {len(pedidos)} pedidos, {len(devoluciones)} devoluciones.")
    cur.close()
    conn.close()


# ---------------------------------------------------------------------------
# Archivos históricos (Bronze vendrá de aquí)
# ---------------------------------------------------------------------------

def _fmt_fecha_mixta(d: datetime) -> str:
    """Formatos heterogéneos a propósito: Silver debe estandarizar."""
    estilo = random.randint(0, 3)
    if estilo == 0:
        return d.strftime("%Y-%m-%d")
    if estilo == 1:
        return d.strftime("%d/%m/%Y")
    if estilo == 2:
        return d.strftime("%d-%m-%Y")
    return d.strftime("%d.%m.%Y")


def _fmt_precio_mixto(precio: float) -> str:
    """Precio como texto sucio: separador decimal español, símbolos, etc."""
    estilo = random.randint(0, 3)
    if estilo == 0:
        return f"{precio:.2f}"
    if estilo == 1:
        return f"{precio:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    if estilo == 2:
        return f"€{precio:.2f}"
    return f"{precio:.2f} EUR"


def seed_historico_ventas(force: bool = False) -> None:
    destino = config.RAW_ROOT / "historico_ventas"
    destino.mkdir(parents=True, exist_ok=True)
    archivos = [destino / "ventas_2023.csv", destino / "ventas_2024.csv"]
    if all(a.exists() for a in archivos) and not force:
        print("[seed] Histórico de ventas ya existe — se omite.")
        return

    conn = connect()
    cur = conn.cursor()
    cur.execute("SELECT id FROM variantes ORDER BY id")
    variant_ids = [r[0] for r in cur.fetchall()]
    cur.execute("SELECT v.id, v.sku, p.precio FROM variantes v JOIN productos p ON p.id = v.producto_id")
    sku_precio = {r[0]: (r[1], float(r[2]) or 49.9) for r in cur.fetchall()}
    cur.close()
    conn.close()

    for anio, n_filas, fname in ((2023, 1500, archivos[0]), (2024, 1800, archivos[1])):
        with open(fname, "w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(["fecha", "orden_ref", "sku", "cantidad", "precio_unitario", "canal", "ciudad"])
            for i in range(n_filas):
                vid = random.choice(variant_ids)
                sku, precio = sku_precio[vid]
                # picos de venta en navidad / rebajas
                mes = random.choices(
                    range(1, 13),
                    weights=[7, 6, 8, 8, 9, 8, 7, 7, 8, 9, 13, 10],
                )[0]
                dia = random.randint(1, 28)
                fecha = datetime(anio, mes, dia)
                w.writerow([
                    _fmt_fecha_mixta(fecha),
                    f"H-{anio}-{i:05d}",
                    sku,
                    random.choices([1, 2, 3], weights=[80, 16, 4])[0],
                    _fmt_precio_mixto(precio),
                    random.choice(CANALES_HIST),
                    random.choice(CIUDADES),
                ])
    print(f"[seed] Histórico de ventas: {archivos[0].name}, {archivos[1].name}")


def seed_historico_devoluciones(force: bool = False) -> None:
    destino = config.RAW_ROOT / "devoluciones_historicas.csv"
    if destino.exists() and not force:
        print("[seed] Histórico de devoluciones ya existe — se omite.")
        return
    with open(destino, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["fecha", "orden_ref", "motivo"])
        for i in range(600):
            anio = random.choice([2023, 2024])
            fecha = datetime(anio, random.randint(1, 12), random.randint(1, 28))
            w.writerow([
                _fmt_fecha_mixta(fecha),
                f"H-{anio}-{random.randint(0, 1799):05d}",
                random.choice(MOTIVOS_DEVOLUCION),
            ])
    print(f"[seed] Histórico de devoluciones: {destino.name}")


def seed_proveedores(force: bool = False) -> None:
    destino = config.RAW_ROOT / "proveedores.json"
    if destino.exists() and not force:
        print("[seed] Catálogo de proveedores ya existe — se omite.")
        return
    proveedores = []
    for i in range(1, 13):
        proveedores.append({
            "proveedor_id": f"PROV-{i:03d}",
            "nombre": fake.company(),
            "contacto": fake.email(),
            "categorias": random.sample(list(MODELOS), random.randint(1, 3)),
            "pais": random.choice(["España", "Portugal", "Italia", "China", "Turquía"]),
            "plazo_entrega_dias": random.choice([7, 14, 21, 30, 45]),
        })
    destino.write_text(json.dumps(proveedores, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[seed] Catálogo de proveedores: {destino.name}")


# ---------------------------------------------------------------------------
# Eventos de navegación históricos (JSON Lines -> bronze/eventos_web)
# ---------------------------------------------------------------------------

def _evento(event_type: str, ts: datetime, session_id: str, **kwargs) -> dict:
    ev = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "ts": ts.isoformat(timespec="seconds"),
        "session_id": session_id,
        "user_id": kwargs.pop("user_id", None),
        "producto_id": None,
        "variante_id": None,
        "sku": None,
        "cantidad": None,
        "monto": None,
        "search_query": None,
        "stock_actual": None,
        "motivo": None,
        "canal": kwargs.pop("canal", random.choice(["web", "app"])),
    }
    ev.update(kwargs)
    return ev


def seed_eventos_historicos(force: bool = False) -> None:
    destino = config.RAW_ROOT / "eventos_web"
    destino.mkdir(parents=True, exist_ok=True)
    archivos = sorted(destino.glob("eventos_*.json"))
    if archivos and not force:
        print(f"[seed] Eventos históricos ya existen ({len(archivos)} archivos) — se omite.")
        return

    conn = connect()
    cur = conn.cursor()
    cur.execute("SELECT id, producto_id, sku FROM variantes")
    variantes = cur.fetchall()
    cur.execute("SELECT id FROM clientes")
    cliente_ids = [r[0] for r in cur.fetchall()]
    cur.close()
    conn.close()

    por_producto = {}
    for vid, pid, sku in variantes:
        por_producto.setdefault(pid, []).append((vid, sku))

    ahora = datetime.now()
    inicio = ahora - timedelta(days=21)
    eventos = []
    n_dias = 21
    sesiones_por_dia = 130

    for dia in range(n_dias):
        dia_actual = inicio + timedelta(days=dia)
        for _ in range(sesiones_por_dia):
            session_id = str(uuid.uuid4())
            user_id = random.choice(cliente_ids) if random.random() < 0.7 else None
            canal = random.choices(["web", "app"], weights=[70, 30])[0]
            t = dia_actual.replace(hour=random.randint(8, 23),
                                   minute=random.randint(0, 59),
                                   second=random.randint(0, 59))
            # embudo: vistas >> carrito >> checkout >> compra
            vistas = random.choices([1, 2, 3, 4, 6, 8], weights=[25, 25, 20, 15, 10, 5])[0]
            visto = []
            for _ in range(vistas):
                pid = random.choice(list(por_producto))
                vid, sku = random.choice(por_producto[pid])
                eventos.append(_evento(
                    "product_viewed", t, session_id, user_id=user_id, canal=canal,
                    producto_id=pid, variante_id=vid, sku=sku,
                ))
                visto.append((pid, vid, sku))
                t += timedelta(seconds=random.randint(5, 90))
            if random.random() < 0.35:
                eventos.append(_evento(
                    "search_performed", t, session_id, user_id=user_id, canal=canal,
                    search_query=random.choice(["zapatillas running", "zapatos de vestir",
                                                "sandalias playa", "talla 42", "rebajas"]),
                ))
                t += timedelta(seconds=random.randint(10, 60))
            if visto and random.random() < 0.40:
                pid, vid, sku = random.choice(visto)
                cantidad = random.choices([1, 2], weights=[85, 15])[0]
                monto = round(random.uniform(29.9, 189.9) * cantidad, 2)
                eventos.append(_evento(
                    "added_to_cart", t, session_id, user_id=user_id, canal=canal,
                    producto_id=pid, variante_id=vid, sku=sku, cantidad=cantidad, monto=monto,
                ))
                t += timedelta(seconds=random.randint(30, 300))
                if random.random() < 0.55:
                    eventos.append(_evento(
                        "checkout_started", t, session_id, user_id=user_id, canal=canal,
                        monto=monto,
                    ))
                    t += timedelta(seconds=random.randint(10, 120))
                    if random.random() < 0.62:
                        order_ev = _evento(
                            "order_placed", t, session_id, user_id=user_id, canal=canal,
                            monto=monto, cantidad=cantidad,
                        )
                        eventos.append(order_ev)
                        if random.random() < 0.10:
                            dup = dict(order_ev)
                            dup["ts"] = (t + timedelta(seconds=2)).isoformat(timespec="seconds")
                            eventos.append(dup)   # duplicado a propósito -> silver deduplica
                        t += timedelta(seconds=random.randint(5, 60))
                        if random.random() < 0.92:
                            eventos.append(_evento(
                                "payment_confirmed", t, session_id, user_id=user_id, canal=canal,
                                monto=monto,
                            ))
                        else:
                            eventos.append(_evento(
                                "payment_failed", t, session_id, user_id=user_id, canal=canal,
                                monto=monto, motivo=random.choice(["fondos_insuficientes", "tarjeta_rechazada"]),
                            ))

    # alertas de stock dispersas
    for _ in range(40):
        vid, pid, sku = random.choice(variantes)
        ts = inicio + timedelta(days=random.randint(0, n_dias - 1),
                                hours=random.randint(9, 21))
        eventos.append(_evento(
            "stock_low", ts, str(uuid.uuid4()),
            producto_id=pid, variante_id=vid, sku=sku,
            stock_actual=random.randint(0, 5),
        ))

    eventos.sort(key=lambda e: e["ts"])
    por_dia = {}
    for ev in eventos:
        por_dia.setdefault(ev["ts"][:10], []).append(ev)
    for fecha, evs in sorted(por_dia.items()):
        fname = destino / f"eventos_{fecha}.json"
        with open(fname, "w", encoding="utf-8") as fh:
            for ev in evs:
                fh.write(json.dumps(ev, ensure_ascii=False) + "\n")

    print(f"[seed] Eventos históricos: {len(por_dia)} archivos, {len(eventos)} eventos "
          f"({len(eventos) - sum(1 for e in eventos if not e.get('event_id'))} con id, "
          f"duplicados incluidos).")


def main() -> int:
    force = "--force" in sys.argv
    print("[seed] Generando datos de ejemplo...")
    seed_postgres(force=force)
    seed_historico_ventas(force=force)
    seed_historico_devoluciones(force=force)
    seed_proveedores(force=force)
    seed_eventos_historicos(force=force)
    print("[seed] Listo.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
