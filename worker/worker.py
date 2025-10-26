import os, time, requests
from datetime import datetime, timezone, date
from dateutil import tz
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from typing import Tuple

DB_DSN = os.getenv("DB_DSN", "mysql+pymysql://app:change_me@db:3306/sncbslac")
FROM = os.getenv("FROM_STATION", "Tournai")
TO = os.getenv("TO_STATION", "Bruxelles-Central")
LANG = os.getenv("IRAIL_LANG", "fr")
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "120"))
USER_AGENT = os.getenv("USER_AGENT", "SNCB-Slac/1.0 (+https://sncb.terminalcommun.be)")

engine = create_engine(DB_DSN, pool_pre_ping=True, future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)

from_backend = False
try:
    # Reuse models from backend by path if mounted together; here we re-declare minimal structures.
    pass
except Exception:
    pass

# Minimal models (string-based) to avoid circular import between images
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import String, Integer, BigInteger, Enum, DateTime, Date, ForeignKey, Boolean
from sqlalchemy.orm import relationship

class Base(DeclarativeBase): pass

class Journey(Base):
    __tablename__ = "journeys"
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    vehicle_uri: Mapped[str] = mapped_column(String(255), nullable=False)
    vehicle_name: Mapped[str] = mapped_column(String(64), nullable=False)
    service_date: Mapped["Date"] = mapped_column(Date, nullable=False)
    from_station_uri: Mapped[str] = mapped_column(String(255), nullable=False)
    to_station_uri: Mapped[str] = mapped_column(String(255), nullable=False)
    planned_departure: Mapped["DateTime"] = mapped_column(DateTime, nullable=False)
    planned_arrival: Mapped["DateTime"] = mapped_column(DateTime, nullable=False)
    realtime_departure: Mapped["DateTime | None"] = mapped_column(DateTime, nullable=True)
    realtime_arrival: Mapped["DateTime | None"] = mapped_column(DateTime, nullable=True)
    status: Mapped[str] = mapped_column(Enum("running","completed", name="journey_status"), nullable=False)
    direction: Mapped[str | None] = mapped_column(String(128), nullable=True)

class JourneyStop(Base):
    __tablename__ = "journey_stops"
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    journey_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("journeys.id", ondelete="CASCADE"))
    stop_order: Mapped[int] = mapped_column(Integer, nullable=False)
    station_uri: Mapped[str] = mapped_column(String(255), nullable=False)
    station_name: Mapped[str] = mapped_column(String(128), nullable=False)
    planned_arrival: Mapped["DateTime | None"] = mapped_column(DateTime, nullable=True)
    planned_departure: Mapped["DateTime | None"] = mapped_column(DateTime, nullable=True)
    realtime_arrival: Mapped["DateTime | None"] = mapped_column(DateTime, nullable=True)
    realtime_departure: Mapped["DateTime | None"] = mapped_column(DateTime, nullable=True)
    platform: Mapped[str | None] = mapped_column(String(16), nullable=True)
    arrived: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    left: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    is_extra_stop: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    arrival_canceled: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    departure_canceled: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)

Base.metadata.create_all(bind=engine)

def irail(path, params):
    headers = {
        "Accept": "application/json",
        "User-Agent": USER_AGENT,
    }
    url = f"https://api.irail.be/{path}"
    r = requests.get(url, params=params, headers=headers, timeout=15)
    r.raise_for_status()
    return r.json()

def to_dt(ts):
    # iRail often returns epoch seconds or ISO; normalize if needed. Here assume epoch seconds or ISO-like.
    try:
        # epoch
        return datetime.fromtimestamp(int(ts), tz=tz.gettz("Europe/Brussels")).replace(tzinfo=None)
    except Exception:
        try:
            return datetime.fromisoformat(ts.replace("Z","+00:00")).astimezone(tz.gettz("Europe/Brussels")).replace(tzinfo=None)
        except Exception:
            return None

def parse_vehicle(vehicle_id: str):
    data = irail("vehicle/", {"id": vehicle_id, "format": "json", "lang": LANG})
    stops = []
    last_arrived = False
    planned_departure = None
    planned_arrival = None
    realtime_departure = None
    realtime_arrival = None
    vehicle_name = data.get("vehicle", {}).get("name") or vehicle_id
    direction = data.get("vehicle", {}).get("direction", {}).get("name")

    for idx, st in enumerate(data.get("stops", {}).get("stop", []), start=1):
        station_uri = st.get("stationinfo", {}).get("uri") or st.get("station")
        station_name = st.get("stationinfo", {}).get("name") or st.get("station")
        arr = st.get("arrival", {})
        dep = st.get("departure", {})
        pa = to_dt(arr.get("time")) if arr else None
        pd = to_dt(dep.get("time")) if dep else None
        ra = to_dt(arr.get("realtime")) if arr else None
        rd = to_dt(dep.get("realtime")) if dep else None
        if idx == 1:
            planned_departure = pd or pa
            realtime_departure = rd or ra
        planned_arrival = pa or planned_arrival
        realtime_arrival = ra or realtime_arrival
        platform = dep.get("platform") or arr.get("platform")
        arrived = bool(int(arr.get("arrived", "0"))) if arr else False
        left = bool(int(dep.get("left", "0"))) if dep else False
        is_extra_stop = bool(int(st.get("isExtraStop", "0")))
        arrival_canceled = bool(int(arr.get("canceled", "0"))) if arr else False
        departure_canceled = bool(int(dep.get("canceled", "0"))) if dep else False

        stops.append({
            "stop_order": idx,
            "station_uri": station_uri,
            "station_name": station_name,
            "planned_arrival": pa,
            "planned_departure": pd,
            "realtime_arrival": ra,
            "realtime_departure": rd,
            "platform": platform,
            "arrived": arrived,
            "left": left,
            "is_extra_stop": is_extra_stop,
            "arrival_canceled": arrival_canceled,
            "departure_canceled": departure_canceled
        })

    if stops:
        last = stops[-1]
        last_arrived = last["arrived"]

    from_uri = stops[0]["station_uri"] if stops else None
    to_uri = stops[-1]["station_uri"] if stops else None

    return {
        "vehicle_uri": f"http://irail.be/vehicle/{vehicle_id}" if not vehicle_id.startswith("http") else vehicle_id,
        "vehicle_name": vehicle_name,
        "from_station_uri": from_uri or "",
        "to_station_uri": to_uri or "",
        "planned_departure": planned_departure or datetime.now(),
        "planned_arrival": planned_arrival or datetime.now(),
        "realtime_departure": realtime_departure,
        "realtime_arrival": realtime_arrival,
        "status": "completed" if last_arrived else "running",
        "direction": direction,
        "stops": stops
    }

def collect_once():
    # Query both directions around now
    conns = []
    for a, b in [(FROM, TO), (TO, FROM)]:
        try:
            data = irail("connections/", {"from": a, "to": b, "format": "json", "lang": LANG})
            conns.extend(data.get("connection", data.get("connections", {}).get("connection", [])))
        except Exception as e:
            print("connections error", a, b, e)

    # Extract vehicles
    vehicles = set()
    for c in conns:
        vehicle_id = None
        # Different payloads exist; attempt common fields
        if "vehicle" in c:
            vehicle_id = c["vehicle"].split("/")[-1] if "/" in c["vehicle"] else c["vehicle"]
        elif "vias" in c:
            try:
                vehicle_id = c["vias"]["via"][0]["vehicle"]
            except Exception:
                pass
        if vehicle_id:
            vehicles.add(vehicle_id)

    if not vehicles:
        print("No vehicles found in connections.")
        return

    with SessionLocal() as s:
        for vid in vehicles:
            try:
                parsed = parse_vehicle(vid)
            except Exception as e:
                print("vehicle parse error", vid, e)
                time.sleep(0.4)
                continue

            # Upsert
            from sqlalchemy import select, delete
            # Determine service_date from planned_departure (Europe/Brussels)
            service_date = parsed["planned_departure"].date()

            exists = s.execute(
                select(Journey).where(Journey.vehicle_uri == parsed["vehicle_uri"], Journey.service_date == service_date)
            ).scalars().first()

            if not exists:
                j = Journey(
                    vehicle_uri=parsed["vehicle_uri"],
                    vehicle_name=parsed["vehicle_name"],
                    service_date=service_date,
                    from_station_uri=parsed["from_station_uri"],
                    to_station_uri=parsed["to_station_uri"],
                    planned_departure=parsed["planned_departure"],
                    planned_arrival=parsed["planned_arrival"],
                    realtime_departure=parsed["realtime_departure"],
                    realtime_arrival=parsed["realtime_arrival"],
                    status=parsed["status"],
                    direction=parsed["direction"],
                )
                s.add(j); s.flush()
                jid = j.id
            else:
                exists.vehicle_name = parsed["vehicle_name"]
                exists.from_station_uri = parsed["from_station_uri"]
                exists.to_station_uri = parsed["to_station_uri"]
                exists.planned_departure = parsed["planned_departure"]
                exists.planned_arrival = parsed["planned_arrival"]
                exists.realtime_departure = parsed["realtime_departure"]
                exists.realtime_arrival = parsed["realtime_arrival"]
                exists.status = parsed["status"]
                exists.direction = parsed["direction"]
                jid = exists.id
                s.execute(delete(JourneyStop).where(JourneyStop.journey_id == jid))

            order = 1
            for st in parsed["stops"]:
                s.add(JourneyStop(
                    journey_id=jid,
                    stop_order=order,
                    station_uri=st["station_uri"],
                    station_name=st["station_name"],
                    planned_arrival=st["planned_arrival"],
                    planned_departure=st["planned_departure"],
                    realtime_arrival=st["realtime_arrival"],
                    realtime_departure=st["realtime_departure"],
                    platform=st["platform"],
                    arrived=st["arrived"],
                    left=st["left"],
                    is_extra_stop=st["is_extra_stop"],
                    arrival_canceled=st["arrival_canceled"],
                    departure_canceled=st["departure_canceled"],
                ))
                order += 1
            s.commit()
            time.sleep(0.4)  # keep under ~3 req/s

def main():
    print("Worker started; polling every", POLL_SECONDS, "seconds")
    while True:
        try:
            collect_once()
        except Exception as e:
            print("collect_once error", e)
        time.sleep(POLL_SECONDS)

if __name__ == "__main__":
    main()
