# --- worker/crud.py ---
from sqlalchemy import select, delete
from sqlalchemy.orm import Session
from models import Journey, JourneyStop

def upsert_journey(session: Session, j: Journey, stops: list[JourneyStop]):
    stmt = select(Journey).where(Journey.vehicle_uri == j.vehicle_uri, Journey.service_date == j.service_date)
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
