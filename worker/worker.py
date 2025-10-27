# --- worker/worker.py ---
import os
import time
import json
import logging
import argparse
from datetime import datetime, date, timezone
from typing import Any, Dict, List, Tuple, Optional

import requests
from requests.adapters import HTTPAdapter, Retry
from dateutil import tz

from database import SessionLocal
from models import Journey, JourneyStop
from crud import upsert_journey

# -------------------------
# Config & logging
# -------------------------
FROM_STATION = os.getenv("FROM_STATION", "Tournai")
TO_STATION = os.getenv("TO_STATION", "Bruxelles-Central")
IRAIL_LANG = os.getenv("IRAIL_LANG", "fr")
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "120"))
USER_AGENT = os.getenv("USER_AGENT", "SNCB-Slac/1.0 (+https://sncb.terminalcommun.be)")
TZ = os.getenv("TZ", "Europe/Brussels")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s worker: %(message)s",
)
log = logging.getLogger("worker")

# -------------------------
# HTTP session with retries
# -------------------------
def make_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=4,
        backoff_factor=1.0,
        status_forcelist=[500, 502, 503, 504, 522, 524],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})
    return s

# -------------------------
# iRail helpers
# -------------------------
IRAIl_BASE = "https://api.irail.be"

def fetch_connections(session: requests.Session, frm: str, to: str, lang: str) -> List[Dict[str, Any]]:
    """GET /connections?from=...&to=...&format=json&lang=fr"""
    params = {"from": frm, "to": to, "format": "json", "lang": lang}
    try:
        r = session.get(f"{IRAIl_BASE}/connections/", params=params, timeout=20)
        r.raise_for_status()
        data = r.json()
        conns = data.get("connection") or data.get("connections") or []
        if isinstance(conns, dict):
            conns = [conns]
        if not isinstance(conns, list):
            log.warning("Unexpected connections payload type: %s", type(conns))
            return []
        return conns
    except requests.exceptions.Timeout:
        log.warning("connections timeout %s -> %s", frm, to)
        return []
    except Exception as e:
        log.warning("connections error %s %s: %s", frm, to, e)
        return []

def safe_get(d: Any, *keys, default=None):
    cur = d
    for k in keys:
        if isinstance(cur, dict) and k in cur:
            cur = cur[k]
        else:
            return default
    return cur

def normalize_vehicle_id(vehicle_field: Any) -> Optional[str]:
    """
    iRail peut renvoyer:
      - "BE.NMBS.IC3230" (str)
      - {"name": "IC3230", "id": "BE.NMBS.IC3230", ...} (dict)
    """
    if isinstance(vehicle_field, str):
        return vehicle_field
    if isinstance(vehicle_field, dict):
        vid = vehicle_field.get("id") or vehicle_field.get("@id") or vehicle_field.get("name")
        if isinstance(vid, str):
            # si "IC3230", préfixons BE.NMBS. si absent
            if not vid.startswith("BE."):
                return f"BE.NMBS.{vid}"
            return vid
    return None

def fetch_vehicle_stops(session: requests.Session, vehicle_id: str, lang: str) -> Tuple[str, List[Dict[str, Any]]]:
    """
    GET /vehicle/?id=BE.NMBS.IC3230&format=json&lang=fr
    Retourne (vehicle_name, stops[])
    """
    params = {"id": vehicle_id, "format": "json", "lang": lang}
    try:
        r = session.get(f"{IRAIl_BASE}/vehicle/", params=params, timeout=20)
        r.raise_for_status()
        data = r.json()
        vehicle_name = data.get("vehicle") or data.get("name") or vehicle_id
        stops = data.get("stops", {}).get("stop", [])
        if isinstance(stops, dict):
            stops = [stops]
        if not isinstance(stops, list):
            stops = []
        return str(vehicle_name), stops
    except requests.exceptions.Timeout:
        log.warning("vehicle timeout %s", vehicle_id)
        return vehicle_id, []
    except Exception as e:
        log.warning("vehicle error %s: %s", vehicle_id, e)
        return vehicle_id, []

def to_dt(ts: Any) -> Optional[datetime]:
    """
    Convertit timestamps (epoch secondes / str) en datetime aware Europe/Brussels.
    iRail donne souvent des epoch en str.
    """
    if ts in (None, "", 0, "0"):
        return None
    try:
        # epoch sec string
        v = int(str(ts))
        dt = datetime.fromtimestamp(v, tz=timezone.utc).astimezone(tz.gettz(TZ))
        return dt
    except Exception:
        try:
            # ISO
            return datetime.fromisoformat(str(ts)).astimezone(tz.gettz(TZ))
        except Exception:
            return None

def build_journey_and_stops(conn: Dict[str, Any], session: requests.Session) -> Optional[Tuple[Journey, List[JourneyStop]]]:
    """
    À partir d'une connection iRail, construire le Journey + Stops via endpoint vehicle.
    """
    # departure/arrival blocs
    dep = conn.get("departure", {})
    arr = conn.get("arrival", {})

    # véhicule
    vehicle_field = dep.get("vehicle") or conn.get("vehicle")
    vid = normalize_vehicle_id(vehicle_field)
    if not vid:
        log.warning("skip connection without vehicle id: %s", json.dumps(conn)[:200])
        return None

    # stops détaillés
    vehicle_name, stops_raw = fetch_vehicle_stops(session, vid, IRAIL_LANG)

    # from/to URIs / names
    from_station_uri = dep.get("stationinfo", {}).get("@id") or dep.get("stationinfo", {}).get("id") or ""
    to_station_uri = arr.get("stationinfo", {}).get("@id") or arr.get("stationinfo", {}).get("id") or ""

    # direction (si dispo)
    direction = safe_get(conn, "direction", "name", default=None)

    # horaires planifiés
    planned_dep = to_dt(dep.get("time")) or to_dt(dep.get("scheduledTime"))
    planned_arr = to_dt(arr.get("time")) or to_dt(arr.get("scheduledTime"))

    # horaires réels
    rt_dep = to_dt(dep.get("timeR")) or to_dt(dep.get("realtime"))
    rt_arr = to_dt(arr.get("timeR")) or to_dt(arr.get("realtime"))

    # Statut par défaut : running ; si dernier stop a "arrived=true" => completed
    status = "running"

    # Construire les stops
    stops: List[JourneyStop] = []
    order = 1
    last_arrived = False

    for s in stops_raw:
        # s peut être dict; certaines clés: station, stationinfo, time, scheduledArrivalTime/DepartureTime, delay, platform, canceled, ...
        station_name = s.get("station") or safe_get(s, "stationinfo", "name") or "?"
        station_uri = safe_get(s, "stationinfo", "@id", default=None) or safe_get(s, "stationinfo", "id", default="")

        planned_arrival = to_dt(s.get("arrivalTime")) or to_dt(s.get("time"))  # fallback
        planned_departure = to_dt(s.get("departureTime")) or None

        realtime_arrival = to_dt(s.get("arrivalTimeR")) or None
        realtime_departure = to_dt(s.get("departureTimeR")) or None

        platform = (s.get("platform") or {}).get("name") if isinstance(s.get("platform"), dict) else s.get("platform")

        arrived = bool(s.get("arrived")) if "arrived" in s else False
        left = bool(s.get("left")) if "left" in s else False
        is_extra = bool(s.get("isExtraStop")) if "isExtraStop" in s else False
        arr_canceled = bool(s.get("arrivalCanceled")) if "arrivalCanceled" in s else False
        dep_canceled = bool(s.get("departureCanceled")) if "departureCanceled" in s else False

        last_arrived = arrived  # mis à jour à chaque itération
        stops.append(JourneyStop(
            journey_id=0,  # renseigné par upsert
            stop_order=order,
            station_uri=station_uri,
            station_name=station_name,
            planned_arrival=planned_arrival,
            planned_departure=planned_departure,
            realtime_arrival=realtime_arrival,
            realtime_departure=realtime_departure,
            platform=platform if isinstance(platform, str) else None,
            arrived=arrived,
            left=left,
            is_extra_stop=is_extra,
            arrival_canceled=arr_canceled,
            departure_canceled=dep_canceled,
        ))
        order += 1

    if stops:
        status = "completed" if last_arrived else "running"

    # Dates de service (date locale)
    service_dt = planned_dep or datetime.now(tz=tz.gettz(TZ))
    service_date = service_dt.date()

    j = Journey(
        vehicle_uri=f"http://irail.be/vehicle/{vehicle_name}" if vehicle_name else vid,
        vehicle_name=str(vehicle_name or vid),
        service_date=service_date,
        from_station_uri=from_station_uri or "",
        to_station_uri=to_station_uri or "",
        planned_departure=planned_dep or service_dt,
        planned_arrival=planned_arr or (planned_dep or service_dt),
        realtime_departure=rt_dep,
        realtime_arrival=rt_arr,
        status=status,
        direction=direction,
    )

    return j, stops

# -------------------------
# Main loop
# -------------------------
def process_once(session: requests.Session) -> int:
    """Retourne le nombre de trajets upsertés."""
    total = 0
    # Aller
    conns_fwd = fetch_connections(session, FROM_STATION, TO_STATION, IRAIL_LANG)
    # Retour
    conns_bwd = fetch_connections(session, TO_STATION, FROM_STATION, IRAIL_LANG)

    with SessionLocal() as s:
        for conn in (conns_fwd + conns_bwd):
            try:
                built = build_journey_and_stops(conn, session)
                if not built:
                    continue
                j, stops = built
                upsert_journey(s, j, stops)
                s.commit()
                total += 1
            except Exception as e:
                log.warning("upsert error: %s", e)

    return total

def main():
    parser = argparse.ArgumentParser(description="SNCB Slac worker (iRail poller)")
    parser.add_argument("--once", action="store_true", help="Exécuter un seul cycle puis quitter")
    parser.add_argument("--debug", action="store_true", help="Log niveau DEBUG")
    args = parser.parse_args()
    if args.debug:
        log.setLevel(logging.DEBUG)

    session = make_session()
    if args.once:
        n = process_once(session)
        log.info("cycle inserted/updated: %d", n)
        return

    # Boucle infinie
    while True:
        try:
            n = process_once(session)
            log.info("cycle inserted/updated: %d", n)
        except Exception as e:
            log.error("fatal cycle error: %s", e)
        time.sleep(POLL_SECONDS)

if __name__ == "__main__":
    main()
