# --- worker/main.py ---
import os
import time
import logging
from datetime import datetime, date
from dateutil import tz
from typing import Any, Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter, Retry
from urllib.parse import quote

from sqlalchemy.orm import Session
from sqlalchemy import select, delete

from shared.database import SessionLocal, engine
from shared.models import Base, Journey, JourneyStop

# ==========================================
# Configuration & Logging
# ==========================================
FROM_STATION = os.getenv("FROM_STATION", "Tournai")
TO_STATION = os.getenv("TO_STATION", "Bruxelles-Central")
IRAIL_LANG = os.getenv("IRAIL_LANG", "fr")
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "120"))
USER_AGENT = os.getenv("USER_AGENT", "SNCB-Slac/1.0 (+https://sncb.terminalcommun.be)")
TZ = os.getenv("TZ", "Europe/Brussels")
DEBUG = os.getenv("DEBUG", "false").lower() in ("1", "true", "yes")

LOCAL_TZ = tz.gettz(TZ) or tz.gettz("Europe/Brussels")

logging.basicConfig(
    level=logging.DEBUG if DEBUG else logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("worker")

# ==========================================
# DB init
# ==========================================
Base.metadata.create_all(bind=engine)

# ==========================================
# HTTP client (retries, headers)
# ==========================================
BASE = "https://api.irail.be"

def build_http() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "application/json",
    })
    retries = Retry(
        total=4,
        connect=4,
        read=4,
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

def get_json(path: str, params: Dict[str, Any], timeout: int = 20) -> Optional[Dict[str, Any]]:
    """GET JSON avec retries + logs debug."""
    url = f"{BASE}{path}"
    try:
        r = HTTP.get(url, params=params, timeout=timeout)
        if DEBUG:
            log.debug("HTTP GET %s params=%s -> %s", r.url, params, r.status_code)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.RequestException as e:
        log.warning("HTTP error on %s: %s", url, e)
        return None
    except ValueError:
        log.warning("JSON decode error on %s", url)
        return None

# ==========================================
# Helpers iRail
# ==========================================
def ts_to_dt(ts: Optional[str | int]) -> Optional[datetime]:
    """Convertit un timestamp (s) en datetime naive en TZ locale."""
    if ts is None:
        return None
    try:
        ts = int(ts)
    except Exception:
        return None
    dt = datetime.fromtimestamp(ts, tz=LOCAL_TZ)
    return dt.replace(tzinfo=None)

def list_connections(_from: str, _to: str) -> List[Dict[str, Any]]:
    """/connections : renvoie la liste des connexions."""
    now_local = datetime.now(LOCAL_TZ)
    params = {
        "from": _from,
        "to": _to,
        "format": "json",
        "lang": IRAIL_LANG,
        "time": now_local.strftime("%H%M"),
        "date": now_local.strftime("%d%m%y"),
        "typeOfTransport": "train",
        "results": 6,
    }
    data = get_json("/connections/", params)
    if not isinstance(data, dict):
        if DEBUG:
            log.debug("connections: data is not dict: %r", data)
        return []
    conns = data.get("connection", [])  # list attendu
    if isinstance(conns, dict):
        conns = [conns]
    if not isinstance(conns, list):
        return []
    if DEBUG:
        log.debug("connections %s -> %s : %d result(s)", _from, _to, len(conns))
    return conns

def vehicle_stops(vehicle_id: str, service_date: date) -> List[Dict[str, Any]]:
    """/vehicle : renvoie la liste normalisée des arrêts pour ce véhicule/date."""
    # requests encode automatiquement, mais on protège si id est une URI complète
    params = {
        "id": vehicle_id,
        "date": service_date.strftime("%Y%m%d"),
        "format": "json",
        "lang": IRAIL_LANG,
    }
    data = get_json("/vehicle/", params)
    if not isinstance(data, dict) or "vehicle" not in data:
        if DEBUG:
            log.debug("vehicle %s: réponse invalide", vehicle_id)
        return []

    v = data["vehicle"]
    stops = v.get("stops", {}).get("stop", [])
    if isinstance(stops, dict):
        stops = [stops]
    if not isinstance(stops, list):
        stops = []

    out: List[Dict[str, Any]] = []
    for idx, s in enumerate(stops, start=1):
        if not isinstance(s, dict):
            continue

        # Nom/URI station robustes
        station_name = s.get("station") or s.get("stationname") or ""
        stationinfo = s.get("stationinfo") if isinstance(s.get("stationinfo"), dict) else {}
        station_uri = stationinfo.get("@id") if isinstance(stationinfo, dict) else ""

        # Plateforme : parfois {"name": "..."} ou {"$": "..."} ou string
        platform = None
        plat = s.get("platform")
        if isinstance(plat, dict):
            platform = plat.get("$") or plat.get("name")
        elif isinstance(plat, str):
            platform = plat

        # Arrivée/départ planifiés et réels (iRail met souvent 'time' + 'realtime')
        planned_arrival = ts_to_dt(s.get("time")) if s.get("arrival") else None
        planned_departure = ts_to_dt(s.get("time")) if s.get("departure") else None
        realtime_arrival = ts_to_dt(s.get("realtime")) if s.get("arrival") else None
        realtime_departure = ts_to_dt(s.get("realtime")) if s.get("departure") else None

        def as_bool(v: Any) -> bool:
            if isinstance(v, bool):
                return v
            if isinstance(v, int):
                return v != 0
            if isinstance(v, str):
                return v.strip() in ("1", "true", "True", "yes", "YES")
            return False

        out.append({
            "stop_order": idx,
            "station_uri": station_uri or "",
            "station_name": station_name or "",
            "planned_arrival": planned_arrival,
            "planned_departure": planned_departure,
            "realtime_arrival": realtime_arrival,
            "realtime_departure": realtime_departure,
            "platform": str(platform) if platform is not None else None,
            "arrived": as_bool(s.get("arrived")),
            "left": as_bool(s.get("left")),
            "is_extra_stop": as_bool(s.get("extra")),
            "arrival_canceled": as_bool(s.get("canceled")) if s.get("arrival") else False,
            "departure_canceled": as_bool(s.get("canceled")) if s.get("departure") else False,
        })
    if DEBUG:
        log.debug("vehicle %s: %d stop(s)", vehicle_id, len(out))
    return out

# ==========================================
# Upsert
# ==========================================
def upsert_journey(session: Session, j: Journey, stops: List[JourneyStop]) -> int:
    """Upsert par (vehicle_uri, service_date) et remplace les stops."""
    existing = session.execute(
        select(Journey).where(
            Journey.vehicle_uri == j.vehicle_uri,
            Journey.service_date == j.service_date
        )
    ).scalars().first()

    if existing:
        # Mise à jour des champs
        for attr in [
            "vehicle_name", "from_station_uri", "to_station_uri",
            "planned_departure", "planned_arrival",
            "realtime_departure", "realtime_arrival",
            "status", "direction"
        ]:
            setattr(existing, attr, getattr(j, attr))
        # Remplace les stops
        session.execute(delete(JourneyStop).where(JourneyStop.journey_id == existing.id))
        session.flush()
        for s in stops:
            s.journey_id = existing.id
            session.add(s)
        jid = existing.id
    else:
        session.add(j)
        session.flush()
        for s in stops:
            s.journey_id = j.id
            session.add(s)
        jid = j.id
    return jid

# ==========================================
# Run logic
# ==========================================
def parse_vehicle_fields(c: Dict[str, Any]) -> tuple[Optional[str], Optional[str]]:
    """Retourne (vehicle_id, vehicle_name) à partir d'une connexion iRail."""
    v = c.get("vehicle")
    if isinstance(v, dict):
        vehicle_id = v.get("@id") or v.get("id") or v.get("name")
        vehicle_name = v.get("name") or vehicle_id or "UNKNOWN"
        return vehicle_id, vehicle_name
    if isinstance(v, str):
        # ex "BE.NMBS.IC1909"
        return v, v
    vinfo = c.get("vehicleinfo")
    if isinstance(vinfo, dict):
        vehicle_id = vinfo.get("@id") or vinfo.get("id") or vinfo.get("name")
        vehicle_name = vinfo.get("name") or vehicle_id or "UNKNOWN"
        return vehicle_id, vehicle_name
    return None, None

def run_once() -> None:
    """Effectue un cycle de collecte aller/retour et enregistre en DB."""
    total_upserts = 0
    pairs = [
        (FROM_STATION, TO_STATION, f"{FROM_STATION} → {TO_STATION}"),
        (TO_STATION, FROM_STATION, f"{TO_STATION} → {FROM_STATION}"),
    ]

    with SessionLocal() as s:
        for src, dst, label in pairs:
            conns = list_connections(src, dst)
            if not conns:
                log.info("Aucune connexion (%s)", label)
                continue

            for idx, c in enumerate(conns):
                if not isinstance(c, dict):
                    if DEBUG:
                        log.debug("Connexion %d ignorée (type %s)", idx, type(c).__name__)
                    continue

                # départ / arrivée (dict attendus)
                dep = c.get("departure") if isinstance(c.get("departure"), dict) else {}
                arr = c.get("arrival")  if isinstance(c.get("arrival"),  dict) else {}

                # véhicule
                vehicle_id, vehicle_name = parse_vehicle_fields(c)
                if not vehicle_id:
                    if DEBUG:
                        log.debug("Connexion %d: pas de vehicle id, skip. c.keys=%s", idx, list(c.keys()))
                    continue

                # date de service
                service_ts = dep.get("time") or arr.get("time") or c.get("time")
                service_dt = ts_to_dt(service_ts) or datetime.now(LOCAL_TZ).replace(tzinfo=None)
                service_d = service_dt.date()

                # horaires
                planned_dep = ts_to_dt(dep.get("time"))
                planned_arr = ts_to_dt(arr.get("time"))
                real_dep = ts_to_dt(dep.get("realtime"))
                real_arr = ts_to_dt(arr.get("realtime"))

                # statut
                status = "running"
                if (isinstance(arr.get("arrived"), (bool, int)) and bool(arr.get("arrived"))) \
                   or (real_arr and planned_arr and real_arr >= planned_arr):
                    status = "completed"

                # URIs stations si disponibles
                def station_uri_of(d: Dict[str, Any]) -> str:
                    si = d.get("stationinfo")
                    if isinstance(si, dict):
                        return si.get("@id") or ""
                    return ""
                from_uri = station_uri_of(dep)
                to_uri   = station_uri_of(arr)

                # Récup arrêt détaillés /vehicle
                stops_dicts = vehicle_stops(vehicle_id, service_d)
                stops: List[JourneyStop] = []
                for sd in stops_dicts:
                    stops.append(JourneyStop(
                        stop_order       = sd["stop_order"],
                        station_uri      = sd["station_uri"],
                        station_name     = sd["station_name"],
                        planned_arrival  = sd["planned_arrival"],
                        planned_departure= sd["planned_departure"],
                        realtime_arrival = sd["realtime_arrival"],
                        realtime_departure=sd["realtime_departure"],
                        platform         = sd["platform"],
                        arrived          = sd["arrived"],
                        left             = sd["left"],
                        is_extra_stop    = sd["is_extra_stop"],
                        arrival_canceled = sd["arrival_canceled"],
                        departure_canceled=sd["departure_canceled"],
                    ))

                # Objet trajet
                j = Journey(
                    vehicle_uri        = str(vehicle_id),
                    vehicle_name       = str(vehicle_name)[:64],
                    service_date       = service_d,
                    from_station_uri   = from_uri or "",
                    to_station_uri     = to_uri or "",
                    planned_departure  = planned_dep or service_dt,
                    planned_arrival    = planned_arr or service_dt,
                    realtime_departure = real_dep,
                    realtime_arrival   = real_arr,
                    status             = status,
                    direction          = label,
                )

                try:
                    jid = upsert_journey(s, j, stops)
                    total_upserts += 1
                    if DEBUG:
                        log.debug(
                            "Upsert OK: jid=%s veh=%s (%s) stops=%d status=%s",
                            jid, vehicle_id, vehicle_name, len(stops), status
                        )
                except Exception as e:
                    log.exception("Upsert échoué pour veh=%s (%s): %s", vehicle_id, vehicle_name, e)

        s.commit()

    log.info("Cycle terminé. Upserts=%d", total_upserts)

def main() -> None:
    log.info(
        "Worker démarré: %s -> %s (lang=%s, poll=%ss, debug=%s)",
        FROM_STATION, TO_STATION, IRAIL_LANG, POLL_SECONDS, DEBUG
    )
    while True:
        try:
            run_once()
        except Exception as e:
            log.exception("Erreur cycle: %s", e)
        time.sleep(POLL_SECONDS)

if __name__ == "__main__":
    main()
