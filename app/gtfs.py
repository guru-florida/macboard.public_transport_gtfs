"""
GTFS data loader and in-memory index.

Responsibilities:
- Download the GTFS zip from config URL, using HEAD + ETag/Last-Modified to
  skip re-downloads when the feed has not changed.
- Cache the zip locally (adjacent to the process cwd, or a configured path).
- Parse the relevant GTFS text files into a compact in-memory index.
- Expose stop search and next-departures queries.

GTFS files consumed:
  stops.txt          — stop_id, stop_name, stop_lat, stop_lon
  routes.txt         — route_id, route_short_name, route_long_name
  trips.txt          — trip_id, route_id, service_id, trip_headsign
  stop_times.txt     — trip_id, stop_id, departure_time  (HH:MM:SS, may be >24h)
  calendar.txt       — service_id, monday–sunday booleans, start_date, end_date
  calendar_dates.txt — service_id, date, exception_type (1=add, 2=remove)

Memory notes:
  stop_times.txt is the largest file. We avoid loading unused stops by
  respecting the optional `stop_ids` config filter at index-build time.
"""
from __future__ import annotations

import csv
import io
import logging
import os
import zipfile
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any, Optional

import httpx

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Internal data structures
# ---------------------------------------------------------------------------

@dataclass
class _Stop:
    stop_id: str
    name: str
    lat: Optional[float]
    lon: Optional[float]


@dataclass
class _Route:
    route_id: str
    short_name: str
    long_name: str


@dataclass
class _Trip:
    trip_id: str
    route_id: str
    service_id: str
    headsign: str


@dataclass
class _StopTime:
    trip_id: str
    departure_secs: int   # seconds from midnight (may exceed 86400)


@dataclass
class _Service:
    """Operating days for one service_id."""
    weekdays: set[int]      # 0=Monday … 6=Sunday
    start_date: date
    end_date: date
    added_dates: set[date] = field(default_factory=set)
    removed_dates: set[date] = field(default_factory=set)

    def active_on(self, d: date) -> bool:
        if d in self.removed_dates:
            return False
        if d in self.added_dates:
            return True
        if not (self.start_date <= d <= self.end_date):
            return False
        return d.weekday() in self.weekdays


# ---------------------------------------------------------------------------
# Cache header file (stores ETag / Last-Modified next to the zip)
# ---------------------------------------------------------------------------

def _read_cache_headers(path: str) -> dict[str, str]:
    try:
        with open(path + ".headers") as fh:
            return dict(line.strip().split("=", 1) for line in fh if "=" in line)
    except FileNotFoundError:
        return {}


def _write_cache_headers(path: str, headers: dict[str, str]) -> None:
    with open(path + ".headers", "w") as fh:
        for k, v in headers.items():
            fh.write(f"{k}={v}\n")


# ---------------------------------------------------------------------------
# GTFS zip download
# ---------------------------------------------------------------------------

def _gtfs_path(config: dict[str, Any]) -> str:
    cache_dir = config.get("gtfs_cache_dir", "/tmp/macboard_gtfs")
    os.makedirs(cache_dir, exist_ok=True)
    return os.path.join(cache_dir, "feed.zip")


def download_if_changed(config: dict[str, Any]) -> bool:
    """
    Download the GTFS zip only if the remote content has changed since
    the last download (checked via HEAD + ETag/Last-Modified).

    Returns True if a fresh download was performed.
    """
    url: str = config["gtfs_url"]
    path = _gtfs_path(config)
    cached = _read_cache_headers(path)

    # --- HEAD request to probe for changes ---
    try:
        head_resp = httpx.head(url, timeout=15.0, follow_redirects=True)
        head_resp.raise_for_status()
    except httpx.HTTPError as exc:
        if os.path.exists(path):
            logger.warning("HEAD request failed (%s); using cached GTFS zip.", exc)
            return False
        raise RuntimeError(f"Cannot fetch GTFS feed (HEAD failed): {exc}") from exc

    remote_etag = head_resp.headers.get("etag", "")
    remote_modified = head_resp.headers.get("last-modified", "")

    if os.path.exists(path):
        if remote_etag and cached.get("etag") == remote_etag:
            logger.info("GTFS feed unchanged (ETag match), using cache.")
            return False
        if remote_modified and cached.get("last-modified") == remote_modified:
            logger.info("GTFS feed unchanged (Last-Modified match), using cache.")
            return False

    logger.info("Downloading GTFS feed from %s …", url)
    with httpx.stream("GET", url, timeout=120.0, follow_redirects=True) as resp:
        resp.raise_for_status()
        with open(path, "wb") as fh:
            for chunk in resp.iter_bytes(chunk_size=65536):
                fh.write(chunk)

    _write_cache_headers(path, {"etag": remote_etag, "last-modified": remote_modified})
    logger.info("GTFS feed downloaded → %s", path)
    return True


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def _parse_secs(time_str: str) -> int:
    """Parse GTFS HH:MM:SS (may be > 24:00:00) into seconds from midnight."""
    parts = time_str.strip().split(":")
    h, m, s = int(parts[0]), int(parts[1]), int(parts[2])
    return h * 3600 + m * 60 + s


def _parse_date(s: str) -> date:
    return date(int(s[:4]), int(s[4:6]), int(s[6:8]))


def _csv_rows(zf: zipfile.ZipFile, filename: str) -> list[dict[str, str]]:
    """Return all rows from a CSV file inside the zip, or [] if absent."""
    try:
        with zf.open(filename) as raw:
            text = raw.read().decode("utf-8-sig")  # handle BOM
            return list(csv.DictReader(io.StringIO(text)))
    except KeyError:
        logger.debug("GTFS zip does not contain %s — skipping.", filename)
        return []


# ---------------------------------------------------------------------------
# Main in-memory index
# ---------------------------------------------------------------------------

class GtfsIndex:
    """
    In-memory index of GTFS data for stop search and next-departure queries.
    """

    def __init__(self) -> None:
        self._stops: dict[str, _Stop] = {}
        self._routes: dict[str, _Route] = {}
        self._trips: dict[str, _Trip] = {}
        self._services: dict[str, _Service] = {}
        # stop_id → sorted list of (departure_secs, trip_id)
        self._stop_departures: dict[str, list[tuple[int, str]]] = defaultdict(list)
        self._loaded = False

    def load(self, config: dict[str, Any]) -> None:
        """Parse the cached GTFS zip into memory."""
        path = _gtfs_path(config)
        if not os.path.exists(path):
            raise RuntimeError(f"GTFS zip not found at {path}")

        filter_stop_ids: set[str] | None = None
        raw_filter = config.get("stop_ids")
        if raw_filter:
            filter_stop_ids = {s.strip() for s in str(raw_filter).split(",") if s.strip()}
            logger.info("GTFS stop filter active: %d stop IDs", len(filter_stop_ids))

        logger.info("Parsing GTFS zip …")
        with zipfile.ZipFile(path, "r") as zf:
            self._parse_stops(zf, filter_stop_ids)
            self._parse_routes(zf)
            self._parse_calendar(zf)
            self._parse_calendar_dates(zf)
            self._parse_trips(zf)
            self._parse_stop_times(zf, filter_stop_ids)

        # Sort departures within each stop
        for stop_id in self._stop_departures:
            self._stop_departures[stop_id].sort()

        self._loaded = True
        logger.info(
            "GTFS loaded: %d stops, %d routes, %d trips, %d stop-departure pairs",
            len(self._stops),
            len(self._routes),
            len(self._trips),
            sum(len(v) for v in self._stop_departures.values()),
        )

    def _parse_stops(self, zf: zipfile.ZipFile, filter_ids: set[str] | None) -> None:
        for row in _csv_rows(zf, "stops.txt"):
            sid = row.get("stop_id", "").strip()
            if not sid:
                continue
            if filter_ids and sid not in filter_ids:
                continue
            try:
                lat = float(row["stop_lat"]) if row.get("stop_lat") else None
                lon = float(row["stop_lon"]) if row.get("stop_lon") else None
            except ValueError:
                lat = lon = None
            self._stops[sid] = _Stop(
                stop_id=sid,
                name=row.get("stop_name", sid),
                lat=lat,
                lon=lon,
            )

    def _parse_routes(self, zf: zipfile.ZipFile) -> None:
        for row in _csv_rows(zf, "routes.txt"):
            rid = row.get("route_id", "").strip()
            if rid:
                self._routes[rid] = _Route(
                    route_id=rid,
                    short_name=row.get("route_short_name", ""),
                    long_name=row.get("route_long_name", ""),
                )

    def _parse_calendar(self, zf: zipfile.ZipFile) -> None:
        DAY_FIELDS = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
        for row in _csv_rows(zf, "calendar.txt"):
            sid = row.get("service_id", "").strip()
            if not sid:
                continue
            weekdays = {i for i, d in enumerate(DAY_FIELDS) if row.get(d, "0") == "1"}
            try:
                svc = _Service(
                    weekdays=weekdays,
                    start_date=_parse_date(row["start_date"]),
                    end_date=_parse_date(row["end_date"]),
                )
                self._services[sid] = svc
            except (KeyError, ValueError):
                continue

    def _parse_calendar_dates(self, zf: zipfile.ZipFile) -> None:
        for row in _csv_rows(zf, "calendar_dates.txt"):
            sid = row.get("service_id", "").strip()
            if not sid:
                continue
            try:
                d = _parse_date(row["date"])
                exc_type = int(row.get("exception_type", "0"))
            except (KeyError, ValueError):
                continue
            if sid not in self._services:
                # calendar_dates-only service (no calendar.txt entry)
                self._services[sid] = _Service(
                    weekdays=set(),
                    start_date=date.min,
                    end_date=date.max,
                )
            if exc_type == 1:
                self._services[sid].added_dates.add(d)
            elif exc_type == 2:
                self._services[sid].removed_dates.add(d)

    def _parse_trips(self, zf: zipfile.ZipFile) -> None:
        for row in _csv_rows(zf, "trips.txt"):
            tid = row.get("trip_id", "").strip()
            if tid:
                self._trips[tid] = _Trip(
                    trip_id=tid,
                    route_id=row.get("route_id", ""),
                    service_id=row.get("service_id", ""),
                    headsign=row.get("trip_headsign", ""),
                )

    def _parse_stop_times(self, zf: zipfile.ZipFile, filter_ids: set[str] | None) -> None:
        for row in _csv_rows(zf, "stop_times.txt"):
            sid = row.get("stop_id", "").strip()
            if not sid:
                continue
            if filter_ids and sid not in filter_ids:
                continue
            if sid not in self._stops:
                continue
            dep_str = row.get("departure_time", "").strip()
            if not dep_str:
                dep_str = row.get("arrival_time", "").strip()
            if not dep_str:
                continue
            tid = row.get("trip_id", "").strip()
            if not tid or tid not in self._trips:
                continue
            try:
                secs = _parse_secs(dep_str)
            except (ValueError, IndexError):
                continue
            self._stop_departures[sid].append((secs, tid))

    # ── Public query API ──────────────────────────────────────────────────

    def search_stops(self, q: str, limit: int = 20) -> list[_Stop]:
        """Return stops whose name contains `q` (case-insensitive)."""
        q_lower = q.lower()
        results = [
            s for s in self._stops.values()
            if q_lower in s.name.lower()
        ]
        results.sort(key=lambda s: s.name)
        return results[:limit]

    def get_stop(self, stop_id: str) -> Optional[_Stop]:
        return self._stops.get(stop_id)

    def next_departures(
        self,
        stop_id: str,
        limit: int = 5,
        at: Optional[datetime] = None,
    ) -> list[dict[str, Any]]:
        """
        Return the next `limit` departures from `stop_id` at or after `at`
        (defaults to now). Returns a list of dicts for use in constructing
        Departure schema objects.

        GTFS convention: dep_secs is seconds since midnight on the *service date*.
        dep_secs may exceed 86400 for trips that run past midnight — the actual
        clock time is service_date + timedelta(seconds=dep_secs).

        We search trips from two service dates:
        - today (handles most departures, including today's post-midnight ones)
        - yesterday (handles yesterday's post-midnight trips that haven't left yet)
        """
        if stop_id not in self._stop_departures:
            return []

        reference = at or datetime.now()
        midnight = datetime.combine(reference.date(), datetime.min.time())

        # Each candidate: (departure_datetime, route_id, route_short_name, route_long_name, headsign)
        candidates: list[tuple[datetime, str, str, str, str]] = []

        for service_day_offset in (0, -1):
            service_date = reference.date() + timedelta(days=service_day_offset)
            service_midnight = datetime.combine(service_date, datetime.min.time())

            svc_ids_checked: set[str] = set()

            for dep_secs, trip_id in self._stop_departures[stop_id]:
                dep_dt = service_midnight + timedelta(seconds=dep_secs)

                if dep_dt < reference:
                    continue

                trip = self._trips.get(trip_id)
                if trip is None:
                    continue

                # Avoid re-checking the same service_id multiple times per day
                cache_key = trip.service_id
                if cache_key in svc_ids_checked:
                    # We already know this service is active/inactive for this date — trust it
                    pass
                svc = self._services.get(trip.service_id)
                if svc is None or not svc.active_on(service_date):
                    continue

                route = self._routes.get(trip.route_id)
                candidates.append((
                    dep_dt,
                    trip.route_id,
                    route.short_name if route else "",
                    route.long_name if route else "",
                    trip.headsign,
                ))

        candidates.sort(key=lambda x: x[0])
        candidates = candidates[:limit]

        return [
            {
                "route_id": d[1],
                "route_short_name": d[2] or None,
                "route_long_name": d[3] or None,
                "trip_headsign": d[4] or None,
                "departure_time": d[0].isoformat(timespec="seconds"),
                "realtime": False,
            }
            for d in candidates
        ]

    @property
    def loaded(self) -> bool:
        return self._loaded


# ---------------------------------------------------------------------------
# Module-level singleton (populated at adapter startup)
# ---------------------------------------------------------------------------

_index: GtfsIndex = GtfsIndex()


def get_index() -> GtfsIndex:
    return _index


def startup(config: dict[str, Any]) -> None:
    """Download (if needed) and parse the GTFS feed. Called once at startup."""
    download_if_changed(config)
    _index.load(config)
