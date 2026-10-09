"""Consumo de una API REST externa -> Bronze.

Ingesta el catálogo de zapatos de una API pública (por defecto DummyJSON,
`https://dummyjson.com`) para enriquecer y contrastar el catálogo propio.

Características del cliente HTTP:
- `requests.Session` con reintentos y backoff (429/5xx) y timeout configurable.
- Cabeceras `Accept` / `User-Agent` y autenticación opcional tipo Bearer
  (`API_TOKEN` -> `Authorization: Bearer <token>`).
- Paginación real por `limit`/`skip` sobre cada categoría (`API_CATEGORIAS`),
  deteniéndose al alcanzar el `total` que informa la API.

Los datos crudos se guardan tal cual, un producto por línea (JSON Lines), en
`lake/bronze/api_catalogo/<categoria>.jsonl` añadiendo metadatos de ingesta
(`_source`, `_endpoint`, `_ingested_at`, `_ingest_date`). La escritura es
idempotente: cada ejecución reemplaza la ingesta anterior.

Uso:  python -m app.api
"""

from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from app import config


class ApiClient:
    """Cliente HTTP con reintentos, timeout y autenticación opcional."""

    def __init__(self) -> None:
        self.session = requests.Session()
        retry = Retry(
            total=config.API_REINTENTOS,
            backoff_factor=0.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET"]),
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def _headers(self) -> dict:
        headers = {
            "Accept": "application/json",
            "User-Agent": config.API_USER_AGENT,
        }
        if config.API_TOKEN:
            valor = f"{config.API_AUTH_PREFIX} {config.API_TOKEN}".strip()
            headers[config.API_AUTH_HEADER] = valor
        return headers

    def get_json(self, path: str, params: dict | None = None) -> dict:
        url = f"{config.API_BASE_URL}/{path.lstrip('/')}"
        resp = self.session.get(
            url,
            headers=self._headers(),
            params=params,
            timeout=config.API_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json()


def fetch_categoria(client: ApiClient, categoria: str) -> list[dict]:
    """Descarga paginada (limit/skip) de todos los productos de una categoría."""
    path = f"{config.API_RECURSO}/{categoria}"
    productos: list[dict] = []
    skip = 0
    for _ in range(config.API_MAX_PAGINAS):
        params = {"limit": config.API_PAGE_SIZE, "skip": skip}
        if config.API_SELECT:
            params["select"] = config.API_SELECT
        data = client.get_json(path, params)
        lote = data.get("products") or data.get("data") or []
        if not lote:
            break
        for producto in lote:
            registro = dict(producto)
            registro["_categoria_api"] = categoria
            productos.append(registro)
        skip += len(lote)
        total = data.get("total")
        if total is not None and skip >= int(total):
            break
        if len(lote) < config.API_PAGE_SIZE:
            break
    return productos


def _destino() -> Path:
    return config.bronze_path("api_catalogo")


def ingest_catalogo() -> int:
    """Consume la API y escribe el crudo en Bronze. Devuelve nº de productos."""
    if not config.API_ENABLED:
        print("[api] API_ENABLED=0 — se omite el consumo de la API.")
        return 0

    client = ApiClient()
    destino = _destino()
    destino.mkdir(parents=True, exist_ok=True)
    for viejo in destino.glob("*.jsonl"):
        viejo.unlink()

    now = datetime.now()
    ingested_at = now.isoformat(timespec="seconds")
    ingest_date = now.strftime("%Y-%m-%d")

    total = 0
    for categoria in config.API_CATEGORIAS:
        productos = fetch_categoria(client, categoria)
        fname = destino / f"{categoria}.jsonl"
        with open(fname, "w", encoding="utf-8") as fh:
            for producto in productos:
                producto["_source"] = f"api:{config.API_BASE_URL}"
                producto["_endpoint"] = f"/{config.API_RECURSO}/{categoria}"
                producto["_ingested_at"] = ingested_at
                producto["_ingest_date"] = ingest_date
                fh.write(json.dumps(producto, ensure_ascii=False) + "\n")
        print(f"[api] {categoria}: {len(productos)} productos")
        total += len(productos)

    print(f"[api] Total: {total} productos desde {config.API_BASE_URL}")
    return total


def main() -> int:
    try:
        ingest_catalogo()
        return 0
    except requests.RequestException as exc:
        print(f"[api] Error al consumir la API: {exc}", file=sys.stderr)
        return 1
    finally:
        sys.stdout.flush()


if __name__ == "__main__":
    raise SystemExit(main())
