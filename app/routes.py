"""
transport.schedule interface routes.

Mounted by create_adapter_app() under /api/interfaces/transport.schedule/.

Endpoints:
  GET /stops/search?q=<text>[&limit=<n>]
      Search stops by name fragment.

  GET /stops/{stop_id}
      Return a single stop record.

  GET /stops/{stop_id}/departures?limit=<n>[&at=<iso_datetime>]
      Return the next N departures from a stop, optionally starting from `at`.
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from macboard_adapter.models import (
    Departure,
    StopDeparturesResponse,
    StopSearchResponse,
    TransportStop,
)

from app import gtfs as gtfs_module

router = APIRouter()


def _to_transport_stop(stop: gtfs_module._Stop) -> TransportStop:
    return TransportStop(
        stop_id=stop.stop_id,
        name=stop.name,
        lat=stop.lat,
        lon=stop.lon,
    )


@router.get("/stops/search", response_model=StopSearchResponse)
async def search_stops(
    q: str = Query(..., min_length=2, description="Name fragment to search for"),
    limit: int = Query(default=20, ge=1, le=100),
) -> StopSearchResponse:
    index = gtfs_module.get_index()
    if not index.loaded:
        raise HTTPException(status_code=503, detail="GTFS data not yet loaded")
    stops = index.search_stops(q, limit=limit)
    return StopSearchResponse(stops=[_to_transport_stop(s) for s in stops])


@router.get("/stops/{stop_id}", response_model=TransportStop)
async def get_stop(stop_id: str) -> TransportStop:
    index = gtfs_module.get_index()
    if not index.loaded:
        raise HTTPException(status_code=503, detail="GTFS data not yet loaded")
    stop = index.get_stop(stop_id)
    if stop is None:
        raise HTTPException(status_code=404, detail=f"Stop '{stop_id}' not found")
    return _to_transport_stop(stop)


@router.get("/stops/{stop_id}/departures", response_model=StopDeparturesResponse)
async def get_departures(
    stop_id: str,
    limit: int = Query(default=5, ge=1, le=50),
    at: Optional[str] = Query(
        default=None,
        description="ISO-8601 datetime to query from (defaults to now)",
    ),
) -> StopDeparturesResponse:
    index = gtfs_module.get_index()
    if not index.loaded:
        raise HTTPException(status_code=503, detail="GTFS data not yet loaded")

    stop = index.get_stop(stop_id)
    if stop is None:
        raise HTTPException(status_code=404, detail=f"Stop '{stop_id}' not found")

    at_dt: Optional[datetime] = None
    if at:
        try:
            at_dt = datetime.fromisoformat(at)
        except ValueError:
            raise HTTPException(status_code=422, detail=f"Invalid `at` datetime: {at!r}")

    raw = index.next_departures(stop_id, limit=limit, at=at_dt)
    departures = [Departure(**d) for d in raw]
    return StopDeparturesResponse(
        stop=_to_transport_stop(stop),
        departures=departures,
    )
