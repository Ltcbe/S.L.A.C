# --- worker/main.py ---
import os
import time
import logging
from datetime import datetime, date
from dateutil import tz

import requests
from requests.adapters import HTTPAdapter, Retry

from sqlalchemy.orm import Session
from sqlalchemy import select, delete

from database import SessionLocal, engine
from models import Base, Journey, JourneyStop

# ---------- Logging ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("worker")

# ---------- Config ----------
FROM_STATION = os.getenv("FROM_STATION", "Tournai")
TO_STATION = os.getenv("TO_STATION", "Bruxelles-Central")
IRAIL_LANG = os.getenv("IRAIL_LANG", "fr")
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "120"))
USER_AGENT = os.getenv("USER_AGENT", "SNCB-Slac/1.0 (+https://sncb.terminalcommun.be)")
TZ = os.getenv("TZ", "Europe/Brussels")

LOCAL_TZ = tz.gettz(TZ) or tz.gettz("Europe/Brussels")

# ---------- DB init ----------
Base.metadata.create_all(bind=engine)

# ---------- HTTP client with retries ----------
def build_http():
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT})
    retries = Retry(
        total=4,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

HTTP = build_http()
BASE = "https://api.irail.be"

def get_json(path: str, params: dict, timeout: int = 20) -> dict | None:
    try:
        r = HTTP.get(f"{BASE}{path}", params=params, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.RequestException as e:
        log.warning("HTTP error on %s: %s", path, e)
        return None
    except ValueError:
        log.warning("JSON decode error on %s", path)
        return None

def ts_to_dt(ts: str | int | None) -> datetime | None:
    if ts is None:
        return None
    try:
        ts = int(ts)
    except Exception:
        return None
    dt = datetime.fromtimestamp(ts, tz=LOCAL_TZ)
    return dt.replace(tzinfo=None)

def list_connections(_from: str, _to: str) -> list[dict]:
    params = {
        "from": _from,
        "to": _to,
        "format": "json",
        "lang": IRAIL_LANG,
        "time": datetime.now(LOCAL_TZ).strftime("%H%M"),
        "date": datetime.now(LOCAL_TZ).strftime("%d%m%y"),
        "typeOfTransport": "train",
        "results": 6,
    }
    data = get_json("/connections/", params)
    conns = data.get("connection", []) if isinstance(data, dict) else []
    if not isinstance(conns, list):
        return []
    return conns

def vehicle_stops(vehicle_id: str, service_date: date) -> list[dict]:
    date_str = service_date.strftime("%Y%m%d")
    params = {"id": vehicle_id, "date": date_str, "format": "json", "lang": IRAIL_LANG}
    data = get_json("/vehicle/", params)
    if not data or "vehicle" not in data:
        return []
    v = data["vehicle"]
    stops = v.get("stops", {}).get("stop", [])
    if isinstance(stops, dict):
        stops = [stops]
    out = []
    for idx, s in enumerate(stops, start=1):
        if not isinstance(s, dict):
            continue
        station_name = s.get("station", "") or s.get("stationname", "") or ""
        station_uri = s.get("stationinfo", {}).get("@id") if isinstance(s.get("stationinfo"), dict) else None
        planned_arrival = ts_to_dt(s.get("time")) if s.get("arrival") else None
        planned_departure = ts_to_dt(s.get("time")) if s.get("departure") else None
        realtime_arrival = ts_to_dt(s.get("realtime")) if s.get("arrival") else None
        realtime_departure = ts_to_dt(s.get("realtime")) if s.get("departure") else None
        platform = (s.get("platform", {}) or {}).get("$") if isinstance(s.get("platform"), dict) else s.get("platform")
        arrived = bool(s.get("arrived")) if isinstance(s.get("arrived"), (bool, int)) else False
        left = bool(s.get("left")) if isinstance(s.get("left"), (bool, int)) else False
        is_extra_stop = bool(s.get("extra")) if isinstance(s.get("extra"), (bool, int)) else False
        arrival_canceled = bool(s.get("canceled")) if s.get("arrival") else False
        departure_canceled = bool(s.get("canceled")) if s.get("departure") else False

        out.append({
            "stop_order": idx,
            "station_uri": station_uri or "",
            "station_name": station_name or "",
            "planned_arrival": planned_arrival,
            "planned_departure": planned_departure,
            "realtime_arrival": realtime_arrival,
            "realtime_departure": realtime_departure,
            "platform": str(platform) if platform is not None else None,
            "arrived": arrived,
            "left": left,
            "is_extra_stop": is_extra_stop,
            "arrival_canceled": arrival_canceled,
            "departure_canceled": departure_canceled,
        })
    return out

def upsert_journey(session: Session, j: Journey, stops: list[JourneyStop]) -> int:
    stmt = select(Journey).where(
        Journey.vehicle_uri == j.vehicle_uri,
        Journey.service_date == j.service_date
    )
    existing = session.execute(stmt).scalars().first()

    if existing:
        for attr in ["vehicle_name","from_station_uri","to_station_uri","planned_departure","planned_arrival",
                     "realtime_departure","realtime_arrival","status","direction"]:
            setattr(existing, attr, getattr(j, attr))
        session.execute(delete(JourneyStop).where(JourneyStop.journey_id == existing.id))
        session.flush()
        for s in stops:
            s.journey_id = existing.id
            session.add(s)
        return existing.id
    else:
        session.add(j)
        session.flush()
        for s in stops:
            s.journey_id = j.id
            session.add(s)
        return j.id

def run_once():
    conns_fwd = list_connections(FROM_STATION, TO_STATION)
    conns_bwd = list_connections(TO_STATION, FROM_STATION)

    total_upserts = 0
    with SessionLocal() as s:
        for direction, conns in [("fwd", conns_fwd), ("bwd", conns_bwd)]:
            if not conns:
                log.info("Aucune connection %s (%s → %s)", direction, FROM_STATION if direction=="fwd" else TO_STATION, TO_STATION if direction=="fwd" else FROM_STATION)
                continue

            for c in conns:
                if not isinstance(c, dict):
                    continue

                dep = c.get("departure", {}) if isinstance(c.get("departure"), dict) else {}
                arr = c.get("arrival", {}) if isinstance(c.get("arrival"), dict) else {}

                vehicle = c.get("vehicle")
                if isinstance(vehicle, dict):
                    vehicle_id = vehicle.get("@id") or vehicle.get("id") or vehicle.get("name")
                    vehicle_name = vehicle.get("name") or vehicle_id or "UNKNOWN"
                elif isinstance(vehicle, str):
                    vehicle_id = vehicle
                    vehicle_name = vehicle
                else:
                    vehicle_id = None
                    vehicle_name = "UNKNOWN"

                if not vehicle_id:
                    log.debug("Connection sans vehicle id, skip")
                    continue

                service_date_ts = dep.get("time") or arr.get("time") or c.get("time")
                service_dt = ts_to_dt(service_date_ts) or datetime.now(LOCAL_TZ).replace(tzinfo=None)
                service_d = service_dt.date()

                planned_departure = ts_to_dt(dep.get("time"))
                planned_arrival = ts_to_dt(arr.get("time"))
                realtime_departure = ts_to_dt(dep.get("realtime"))
                realtime_arrival = ts_to_dt(arr.get("realtime"))

                status = "running"
                if arr.get("arrived") or (realtime_arrival and planned_arrival and realtime_arrival >= planned_arrival):
                    status = "completed"

                from_uri = (dep.get("stationinfo") or {}).get("@id") if isinstance(dep.get("stationinfo"), dict) else ""
                to_uri = (arr.get("stationinfo") or {}).get("@id") if isinstance(arr.get("stationinfo"), dict) else ""

                stops_raw = vehicle_stops(vehicle_id, service_d)
                stops = []
                for sr in stops_raw:
                    st = JourneyStop(
                        stop_order=sr["stop_order"],
                        station_uri=sr["station_uri"],
                        station_name=sr["station_name"],
                        planned_arrival=sr["planned_arrival"],
                        planned_departure=sr["planned_departure"],
                        realtime_arrival=sr["realtime_arrival"],
                        realtime_departure=sr["realtime_departure"],
                        platform=sr["platform"],
                        arrived=sr["arrived"],
                        left=sr["left"],
                        is_extra_stop=sr["is_extra_stop"],
                        arrival_canceled=sr["arrival_canceled"],
                        departure_canceled=sr["departure_canceled"],
                    )
                    stops.append(st)

                j = Journey(
                    vehicle_uri=str(vehicle_id),
                    vehicle_name=str(vehicle_name)[:64],
                    service_date=service_d,
                    from_station_uri=from_uri or "",
                    to_station_uri=to_uri or "",
                    planned_departure=planned_departure or service_dt,
                    planned_arrival=planned_arrival or service_dt,
                    realtime_departure=realtime_departure,
                    realtime_arrival=realtime_arrival,
                    status=status,
                    direction=f"{FROM_STATION} → {TO_STATION}" if direction=="fwd" else f"{TO_STATION} → {FROM_STATION}",
                )

                upsert_journey(s, j, stops)
                total_upserts += 1

        s.commit()

    log.info("Cycle terminé. Upserts=%s", total_upserts)

def main():
    log.info("Worker démarré: %s -> %s (lang=%s, poll=%ss)", FROM_STATION, TO_STATION, IRAIL_LANG, POLL_SECONDS)
    while True:
        try:
            run_once()
        except Exception as e:
            log.exception("Erreur cycle: %s", e)
        time.sleep(POLL_SECONDS)

if __name__ == "__main__":
    main()
