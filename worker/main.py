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

from shared.database import SessionLocal, engine
from shared.models import Base, Journey, JourneyStop

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("worker")

FROM_STATION = os.getenv("FROM_STATION", "Tournai")
TO_STATION = os.getenv("TO_STATION", "Bruxelles-Central")
IRAIL_LANG = os.getenv("IRAIL_LANG", "fr")
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "120"))
USER_AGENT = os.getenv("USER_AGENT", "SNCB-Slac/1.0 (+https://sncb.terminalcommun.be)")
TZ = os.getenv("TZ", "Europe/Brussels")
LOCAL_TZ = tz.gettz(TZ) or tz.gettz("Europe/Brussels")

Base.metadata.create_all(bind=engine)

def build_http():
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT})
    retries = Retry(total=4, backoff_factor=1.5, status_forcelist=[429,500,502,503,504], allowed_methods=["GET"], raise_on_status=False)
    adapter = HTTPAdapter(max_retries=retries)
    s.mount("https://", adapter); s.mount("http://", adapter)
    return s

HTTP = build_http()
BASE = "https://api.irail.be"

def get_json(path: str, params: dict, timeout: int = 20) -> dict | None:
    try:
        r = HTTP.get(f"{BASE}{path}", params=params, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.warning("HTTP error on %s: %s", path, e)
        return None

def ts_to_dt(ts: str | int | None):
    if ts is None: return None
    try: ts = int(ts)
    except Exception: return None
    dt = datetime.fromtimestamp(ts, tz=LOCAL_TZ)
    return dt.replace(tzinfo=None)

def list_connections(_from: str, _to: str) -> list[dict]:
    params = {
        "from": _from, "to": _to, "format": "json", "lang": IRAIL_LANG,
        "time": datetime.now(LOCAL_TZ).strftime("%H%M"),
        "date": datetime.now(LOCAL_TZ).strftime("%d%m%y"),
        "typeOfTransport": "train", "results": 6,
    }
    data = get_json("/connections/", params)
    conns = data.get("connection", []) if isinstance(data, dict) else []
    return conns if isinstance(conns, list) else []

def vehicle_stops(vehicle_id: str, service_date: date) -> list[dict]:
    params = {"id": vehicle_id, "date": service_date.strftime("%Y%m%d"), "format": "json", "lang": IRAIL_LANG}
    data = get_json("/vehicle/", params)
    if not data or "vehicle" not in data:
        return []
    stops = data["vehicle"].get("stops", {}).get("stop", [])
    if isinstance(stops, dict): stops = [stops]
    out = []
    for idx, s in enumerate(stops, start=1):
        if not isinstance(s, dict): continue
        station_name = s.get("station", "") or s.get("stationname", "") or ""
        station_uri = s.get("stationinfo", {}).get("@id") if isinstance(s.get("stationinfo"), dict) else ""
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
            "stop_order": idx, "station_uri": station_uri, "station_name": station_name,
            "planned_arrival": planned_arrival, "planned_departure": planned_departure,
            "realtime_arrival": realtime_arrival, "realtime_departure": realtime_departure,
            "platform": str(platform) if platform is not None else None,
            "arrived": arrived, "left": left, "is_extra_stop": is_extra_stop,
            "arrival_canceled": arrival_canceled, "departure_canceled": departure_canceled,
        })
    return out

def upsert_journey(session: Session, j: Journey, stops: list[JourneyStop]) -> int:
    existing = session.execute(
        select(Journey).where(Journey.vehicle_uri==j.vehicle_uri, Journey.service_date==j.service_date)
    ).scalars().first()
    if existing:
        for attr in ["vehicle_name","from_station_uri","to_station_uri","planned_departure","planned_arrival",
                     "realtime_departure","realtime_arrival","status","direction"]:
            setattr(existing, attr, getattr(j, attr))
        session.execute(delete(JourneyStop).where(JourneyStop.journey_id==existing.id))
        session.flush()
        for s in stops:
            s.journey_id = existing.id
            session.add(s)
        return existing.id
    else:
        session.add(j); session.flush()
        for s in stops:
            s.journey_id = j.id
            session.add(s)
        return j.id

def run_once():
    pairs = [(FROM_STATION, TO_STATION, f"{FROM_STATION} → {TO_STATION}"),
             (TO_STATION, FROM_STATION, f"{TO_STATION} → {FROM_STATION}")]
    total = 0
    with SessionLocal() as s:
        for src, dst, label in pairs:
            conns = list_connections(src, dst)
            if not conns:
                log.info("Aucune connexion (%s)", label)
                continue
            for c in conns:
                if not isinstance(c, dict): continue
                dep = c.get("departure") if isinstance(c.get("departure"), dict) else {}
                arr = c.get("arrival") if isinstance(c.get("arrival"), dict) else {}

                vehicle = c.get("vehicle")
                if isinstance(vehicle, dict):
                    vehicle_id = vehicle.get("@id") or vehicle.get("id") or vehicle.get("name")
                    vehicle_name = vehicle.get("name") or vehicle_id or "UNKNOWN"
                elif isinstance(vehicle, str):
                    vehicle_id = vehicle_name = vehicle
                else:
                    continue

                service_dt = ts_to_dt(dep.get("time") or arr.get("time") or c.get("time")) or datetime.now(LOCAL_TZ).replace(tzinfo=None)
                service_d = service_dt.date()

                planned_dep = ts_to_dt(dep.get("time"))
                planned_arr = ts_to_dt(arr.get("time"))
                real_dep = ts_to_dt(dep.get("realtime"))
                real_arr = ts_to_dt(arr.get("realtime"))

                status = "running"
                if arr.get("arrived") or (real_arr and planned_arr and real_arr >= planned_arr):
                    status = "completed"

                from_uri = (dep.get("stationinfo") or {}).get("@id") if isinstance(dep.get("stationinfo"), dict) else ""
                to_uri = (arr.get("stationinfo") or {}).get("@id") if isinstance(arr.get("stationinfo"), dict) else ""

                stops = [JourneyStop(**sr) for sr in vehicle_stops(vehicle_id, service_d)]

                j = Journey(
                    vehicle_uri=str(vehicle_id), vehicle_name=str(vehicle_name)[:64],
                    service_date=service_d, from_station_uri=from_uri or "", to_station_uri=to_uri or "",
                    planned_departure=planned_dep or service_dt, planned_arrival=planned_arr or service_dt,
                    realtime_departure=real_dep, realtime_arrival=real_arr,
                    status=status, direction=label,
                )

                upsert_journey(s, j, stops)
                total += 1
        s.commit()
    log.info("Cycle terminé. Upserts=%s", total)

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
