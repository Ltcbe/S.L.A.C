# --- worker/main.py ---
import os
import time
import logging
import re
from datetime import datetime, date
from typing import Any, Dict, List, Optional

from dateutil import tz
import requests
from requests.adapters import HTTPAdapter, Retry

from sqlalchemy.orm import Session
from sqlalchemy import select, delete

from shared.database import SessionLocal, engine
from shared.models import Base, Journey, JourneyStop

# =========================================================
# Configuration
# =========================================================
FROM_STATION = os.getenv("FROM_STATION", "Tournai")
TO_STATION = os.getenv("TO_STATION", "Bruxelles-Central")
IRAIL_LANG = os.getenv("IRAIL_LANG", "fr")
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "120"))
USER_AGENT = os.getenv("USER_AGENT", "SNCB-Slac/1.0 (+https://sncb.terminalcommun.be)")
TZ = os.getenv("TZ", "Europe/Brussels")

# Active le DEBUG si DEBUG=true/1/yes OU si LOG_LEVEL=DEBUG
DEBUG = os.getenv("DEBUG", "false").lower() in ("1", "true", "yes")
LOG_LEVEL_ENV = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_LEVEL = logging.DEBUG if (DEBUG or LOG_LEVEL_ENV == "DEBUG") else getattr(logging, LOG_LEVEL_ENV, logging.INFO)

LOCAL_TZ = tz.gettz(TZ) or tz.gettz("Europe/Brussels")

# =========================================================
# Logging
# =========================================================
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("worker")

# =========================================================
# DB init
# =========================================================
Base.metadata.create_all(bind=engine)

# =========================================================
# HTTP client
# =========================================================
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
    url = f"{BASE}{path}"
    try:
        r = HTTP.get(url, params=params, timeout=timeout, allow_redirects=True)
        if log.isEnabledFor(logging.DEBUG):
            log.debug("HTTP GET %s -> %s", r.url, r.status_code)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.RequestException as e:
        log.warning("HTTP error on %s: %s", url, e)
        return None
    except ValueError:
        log.warning("JSON decode error on %s", url)
        return None

# =========================================================
# Helpers iRail
# =========================================================
def ts_to_dt(ts: Optional[str | int]) -> Optional[datetime]:
    """Convertit un timestamp (secondes) en datetime naive local."""
    if ts is None:
        return None
    try:
        ts = int(ts)
    except Exception:
        return None
    dt = datetime.fromtimestamp(ts, tz=LOCAL_TZ)
    return dt.replace(tzinfo=None)

def normalize_vehicle_id(raw: str) -> List[str]:
    """
    Normalise un identifiant véhicule iRail en variantes compatibles /vehicle.
    - Si raw commence par http(s):// -> on garde tel quel.
    - Si raw ressemble à 'BE.NMBS.IC3232' -> on fabrique 'http://irail.be/vehicle/IC3232' + variantes.
    - Si raw ressemble déjà à 'IC3232' -> on fabrique l'URI.
    Retourne une liste ordonnée de candidats à essayer.
    """
    candidates: List[str] = []
    if not raw:
        return candidates

    r = raw.strip()

    # Déjà une URI complète
    if r.startswith("http://") or r.startswith("https://"):
        candidates.append(r)
        # Ajoute le code simple s'il existe (fin d'URI)
        m = re.search(r"/vehicle/([A-Za-z]+[0-9]+)$", r)
        if m:
            code = m.group(1)
            candidates.append(code)  # IC3232
            candidates.append(f"BE.NMBS.{code}")  # BE.NMBS.IC3232
        return candidates

    # BE.NMBS.IC3232 -> IC3232
    m = re.match(r"^BE\.NMBS\.([A-Za-z]+[0-9]+)$", r)
    if m:
        code = m.group(1)  # IC3232
        candidates.append(f"http://irail.be/vehicle/{code}")  # préférée
        candidates.append(code)
        candidates.append(r)
        return candidates

    # Code simple (IC3232)
    if re.match(r"^[A-Za-z]+[0-9]+$", r):
        candidates.append(f"http://irail.be/vehicle/{r}")
        candidates.append(r)
        candidates.append(f"BE.NMBS.{r}")
        return candidates

    # Dernier recours : renvoyer brut
    candidates.append(r)
    return candidates

def list_connections(_from: str, _to: str) -> List[Dict[str, Any]]:
    """Appelle /connections et retourne une liste de connexions (dicts)."""
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
    data = get_json("/connections/", params)  # redirigé vers /v1/connections/
    if not isinstance(data, dict):
        if log.isEnabledFor(logging.DEBUG):
            log.debug("connections: data is not dict: %r", data)
        return []
    conns = data.get("connection", [])
    if isinstance(conns, dict):
        conns = [conns]
    if not isinstance(conns, list):
        conns = []
    if log.isEnabledFor(logging.DEBUG):
        log.debug("connections %s -> %s : %d result(s)", _from, _to, len(conns))
    return conns

def vehicle_stops(vehicle_id_raw: str, service_date: date) -> List[Dict[str, Any]]:
    """
    Appelle /vehicle et normalise la liste des arrêts. Essaie plusieurs variantes d'ID.
    """
    date_str = service_date.strftime("%Y%m%d")
    tried: List[str] = []
    for vid in normalize_vehicle_id(vehicle_id_raw):
        params = {"id": vid, "date": date_str, "format": "json", "lang": IRAIL_LANG}
        data = get_json("/vehicle/", params)
        tried.append(vid)
        if isinstance(data, dict) and "vehicle" in data:
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
                station_name = s.get("station") or s.get("stationname") or ""
                stationinfo = s.get("stationinfo") if isinstance(s.get("stationinfo"), dict) else {}
                station_uri = stationinfo.get("@id") if isinstance(stationinfo, dict) else ""
                plat = s.get("platform")
                platform = plat.get("$") or plat.get("name") if isinstance(plat, dict) else (plat if isinstance(plat, str) else None)
                planned_arrival = ts_to_dt(s.get("time")) if s.get("arrival") else None
                planned_departure = ts_to_dt(s.get("time")) if s.get("departure") else None
                realtime_arrival = ts_to_dt(s.get("realtime")) if s.get("arrival") else None
                realtime_departure = ts_to_dt(s.get("realtime")) if s.get("departure") else None

                def as_bool(v: Any) -> bool:
                    if isinstance(v, bool): return v
                    if isinstance(v, int): return v != 0
                    if isinstance(v, str): return v.strip() in ("1", "true", "True", "yes", "YES")
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
            if log.isEnabledFor(logging.DEBUG):
                log.debug("vehicle %s (tried=%s): %d stop(s)", vehicle_id_raw, tried, len(out))
            return out

    log.warning("vehicle stops: aucune réponse valide pour id=%s (tried=%s)", vehicle_id_raw, tried)
    return []

# =========================================================
# Upsert DB
# =========================================================
def upsert_journey(session: Session, j: Journey, stops: List[JourneyStop]) -> int:
    existing = session.execute(
        select(Journey).where(
            Journey.vehicle_uri == j.vehicle_uri,
            Journey.service_date == j.service_date
        )
    ).scalars().first()

    if existing:
        for attr in [
            "vehicle_name", "from_station_uri", "to_station_uri",
            "planned_departure", "planned_arrival",
            "realtime_departure", "realtime_arrival",
            "status", "direction"
        ]:
            setattr(existing, attr, getattr(j, attr))
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

# =========================================================
# Parsing connexion → (vehicle_id, vehicle_name)
#  v1 place le véhicule sous departure/arrival
# =========================================================
def parse_vehicle_fields(c: Dict[str, Any]) -> tuple[Optional[str], Optional[str]]:
    # Ancien schéma (compat)
    v = c.get("vehicle")
    if isinstance(v, dict):
        vehicle_id = v.get("@id") or v.get("id") or v.get("name")
        vehicle_name = v.get("name") or vehicle_id or "UNKNOWN"
        if vehicle_id:
            return vehicle_id, vehicle_name
    elif isinstance(v, str):
        return v, v

    vinfo = c.get("vehicleinfo")
    if isinstance(vinfo, dict):
        vehicle_id = vinfo.get("@id") or vinfo.get("id") or vinfo.get("name")
        vehicle_name = vinfo.get("name") or vehicle_id or "UNKNOWN"
        if vehicle_id:
            return vehicle_id, vehicle_name

    # Nouveau schéma (v1) : départ
    dep = c.get("departure") if isinstance(c.get("departure"), dict) else {}
    if dep:
        dvinfo = dep.get("vehicleinfo") if isinstance(dep.get("vehicleinfo"), dict) else {}
        if dvinfo:
            vehicle_id = dvinfo.get("@id") or dvinfo.get("id") or dvinfo.get("name")
            vehicle_name = dvinfo.get("name") or vehicle_id or "UNKNOWN"
            if vehicle_id:
                return vehicle_id, vehicle_name
        dv = dep.get("vehicle")
        if isinstance(dv, dict):
            vehicle_id = dv.get("@id") or dv.get("id") or dv.get("name")
            vehicle_name = dv.get("name") or vehicle_id or "UNKNOWN"
            if vehicle_id:
                return vehicle_id, vehicle_name
        elif isinstance(dv, str):
            return dv, dv

    # Fallback : arrivée
    arr = c.get("arrival") if isinstance(c.get("arrival"), dict) else {}
    if arr:
        avinfo = arr.get("vehicleinfo") if isinstance(arr.get("vehicleinfo"), dict) else {}
        if avinfo:
            vehicle_id = avinfo.get("@id") or avinfo.get("id") or avinfo.get("name")
            vehicle_name = avinfo.get("name") or vehicle_id or "UNKNOWN"
            if vehicle_id:
                return vehicle_id, vehicle_name
        av = arr.get("vehicle")
        if isinstance(av, dict):
            vehicle_id = av.get("@id") or av.get("id") or av.get("name")
            vehicle_name = av.get("name") or vehicle_id or "UNKNOWN"
            if vehicle_id:
                return vehicle_id, vehicle_name
        elif isinstance(av, str):
            return av, av

    return None, None

# =========================================================
# Un cycle de collecte
# =========================================================
def run_once() -> None:
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
                    if log.isEnabledFor(logging.DEBUG):
                        log.debug("Connexion %d ignorée (type=%s)", idx, type(c).__name__)
                    continue

                dep = c.get("departure") if isinstance(c.get("departure"), dict) else {}
                arr = c.get("arrival")  if isinstance(c.get("arrival"),  dict) else {}

                vehicle_id_raw, vehicle_name = parse_vehicle_fields(c)
                if not vehicle_id_raw:
                    if log.isEnabledFor(logging.DEBUG):
                        log.debug("Connexion %d: pas de vehicle id, skip. keys=%s", idx, list(c.keys()))
                    continue

                service_ts = dep.get("time") or arr.get("time") or c.get("time")
                service_dt = ts_to_dt(service_ts) or datetime.now(LOCAL_TZ).replace(tzinfo=None)
                service_d = service_dt.date()

                planned_dep = ts_to_dt(dep.get("time"))
                planned_arr = ts_to_dt(arr.get("time"))
                real_dep = ts_to_dt(dep.get("realtime"))
                real_arr = ts_to_dt(arr.get("realtime"))

                status = "running"
                try:
                    arrived_flag = arr.get("arrived")
                    if isinstance(arrived_flag, (bool, int)) and bool(arrived_flag):
                        status = "completed"
                    elif real_arr and planned_arr and real_arr >= planned_arr:
                        status = "completed"
                except Exception:
                    pass

                def station_uri_of(d: Dict[str, Any]) -> str:
                    si = d.get("stationinfo")
                    if isinstance(si, dict):
                        return si.get("@id") or ""
                    return ""

                from_uri = station_uri_of(dep)
                to_uri   = station_uri_of(arr)

                # Récup arrêts avec normalisation d'ID + fallbacks
                stops_dicts = vehicle_stops(vehicle_id_raw, service_d)
                stops: List[JourneyStop] = []
                for sd in stops_dicts:
                    stops.append(JourneyStop(
                        stop_order        = sd["stop_order"],
                        station_uri       = sd["station_uri"],
                        station_name      = sd["station_name"],
                        planned_arrival   = sd["planned_arrival"],
                        planned_departure = sd["planned_departure"],
                        realtime_arrival  = sd["realtime_arrival"],
                        realtime_departure= sd["realtime_departure"],
                        platform          = sd["platform"],
                        arrived           = sd["arrived"],
                        left              = sd["left"],
                        is_extra_stop     = sd["is_extra_stop"],
                        arrival_canceled  = sd["arrival_canceled"],
                        departure_canceled= sd["departure_canceled"],
                    ))

                j = Journey(
                    vehicle_uri        = normalize_vehicle_id(vehicle_id_raw)[0],  # stocke la forme préférée (URI si possible)
                    vehicle_name       = str(vehicle_name)[:64] if vehicle_name else normalize_vehicle_id(vehicle_id_raw)[0],
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
                    if log.isEnabledFor(logging.DEBUG):
                        log.debug(
                            "Upsert OK: jid=%s veh_raw=%s veh_norm=%s stops=%d status=%s",
                            jid, vehicle_id_raw, j.vehicle_uri, len(stops), status
                        )
                except Exception as e:
                    log.exception("Upsert échoué pour veh=%s (%s): %s", vehicle_id_raw, vehicle_name, e)

        s.commit()

    log.info("Cycle terminé. Upserts=%d", total_upserts)

# =========================================================
# Main
# =========================================================
def main() -> None:
    log.info(
        "Worker démarré: %s -> %s (lang=%s, poll=%ss, debug=%s)",
        FROM_STATION, TO_STATION, IRAIL_LANG, POLL_SECONDS, LOG_LEVEL == logging.DEBUG
    )
    while True:
        try:
            run_once()
        except Exception as e:
            log.exception("Erreur cycle: %s", e)
        time.sleep(POLL_SECONDS)

if __name__ == "__main__":
    main()
