"""
Public Transport GTFS adapter — entry point.

This is a query adapter (no sync feeds). It declares the
``transport.schedule`` interface, which MacBoard widgets and settings
panels can call to:

  - search for transit stops
  - look up stop details
  - fetch the next N departures from a stop

Configuration (YAML stored in MacBoard):
  gtfs_url:       https://example.com/gtfs.zip   # required
  gtfs_cache_dir: /tmp/macboard_gtfs             # optional, defaults to /tmp/macboard_gtfs
  stop_ids:       1001,1002,1003                 # optional CSV — filter to specific stops
                                                  # (reduces memory for large feeds)

Run:
  uvicorn app.main:app --host 0.0.0.0 --port 8200 --reload
"""
from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from collections.abc import AsyncGenerator
from typing import Any

import httpx
import yaml
from fastapi import FastAPI

from macboard_adapter.app import create_adapter_app
from macboard_adapter.models import AdapterInterfaceMeta, AdapterMeta, TestConnectionResponse

from app import gtfs as gtfs_module
from app.routes import router as transport_router

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# ---------------------------------------------------------------------------
# Adapter metadata
# ---------------------------------------------------------------------------

_META = AdapterMeta(
    adapter_key="public_transport_gtfs",
    name="Public Transport (GTFS)",
    description=(
        "Reads a GTFS feed and provides stop search and next-departure queries. "
        "Does not sync data into MacBoard — widgets query it directly."
    ),
    version="1.0.0",
    config_example_yaml="""\
# URL of the GTFS Static zip feed (required).
# Examples:
#   PKP Intercity (Poland): https://mkuran.pl/gtfs/intercity.zip
#   ZTM Warsaw:             https://mkuran.pl/gtfs/warsaw.zip
gtfs_url: https://example.com/gtfs.zip

# Local directory for caching the downloaded GTFS zip.
# Defaults to /tmp/macboard_gtfs if omitted.
# gtfs_cache_dir: /tmp/macboard_gtfs

# Optional comma-separated list of stop_ids to index.
# Limits memory usage on large feeds — only these stops will be searchable.
# Leave empty to index all stops.
# stop_ids: "1001,1002,1003"
""",
    feeds=[],   # pure query adapter — no sync feeds
    supported_interfaces=[
        AdapterInterfaceMeta(
            interface="transport.schedule",
            version="1",
            name="Public Transit Schedule",
        )
    ],
)

# ---------------------------------------------------------------------------
# Env-based startup config (no per-instance YAML at adapter boot time)
# ---------------------------------------------------------------------------

def _load_config_from_env() -> dict[str, Any]:
    """
    The adapter has no per-instance MacBoard config at startup time.
    Read from env vars so the GTFS zip can be downloaded and parsed once
    at boot, making the first query instant.
    """
    raw = os.environ.get("GTFS_CONFIG_YAML", "")
    if raw:
        try:
            cfg = yaml.safe_load(raw)
            if isinstance(cfg, dict):
                return cfg
        except yaml.YAMLError:
            pass
    config: dict[str, Any] = {}
    if url := os.environ.get("GTFS_URL"):
        config["gtfs_url"] = url
    if cache := os.environ.get("GTFS_CACHE_DIR"):
        config["gtfs_cache_dir"] = cache
    if stops := os.environ.get("GTFS_STOP_IDS"):
        config["stop_ids"] = stops
    return config


# ---------------------------------------------------------------------------
# Test connection handler
# ---------------------------------------------------------------------------

async def _test_connection(config: dict[str, Any]) -> TestConnectionResponse:
    gtfs_url = config.get("gtfs_url", "").strip()
    if not gtfs_url:
        return TestConnectionResponse(
            success=False,
            message="Config missing required field: gtfs_url",
        )
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.head(gtfs_url, follow_redirects=True)
            resp.raise_for_status()
        size = resp.headers.get("content-length", "unknown")
        return TestConnectionResponse(
            success=True,
            message=f"GTFS feed reachable (content-length: {size} bytes)",
            metadata={"gtfs_url": gtfs_url, "content_length": size},
        )
    except httpx.HTTPError as exc:
        return TestConnectionResponse(
            success=False,
            message=f"Cannot reach GTFS feed: {exc}",
        )


# ---------------------------------------------------------------------------
# Lifespan: download and parse GTFS on startup
# ---------------------------------------------------------------------------

@asynccontextmanager
async def _lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Pre-load GTFS data so the first query is instant."""
    config = _load_config_from_env()
    if config.get("gtfs_url"):
        try:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, gtfs_module.startup, config)
        except Exception as exc:
            logger.error("GTFS startup load failed: %s", exc)
            logger.warning(
                "Adapter started without GTFS data — queries will return 503. "
                "Check GTFS_URL and network connectivity."
            )
    else:
        logger.warning(
            "GTFS_URL not set — GTFS data not pre-loaded. "
            "Set GTFS_URL (or GTFS_CONFIG_YAML) environment variable."
        )
    yield


# ---------------------------------------------------------------------------
# Application
# ---------------------------------------------------------------------------

app = create_adapter_app(
    _META,
    test_connection_fn=_test_connection,
    interface_routers={"transport.schedule": transport_router},
    lifespan=_lifespan,
)
