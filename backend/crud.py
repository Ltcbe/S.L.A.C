from sqlalchemy import select, func, delete
from sqlalchemy.orm import Session
from .models import Journey, JourneyStop

def upsert_journey(session: Session, j: Journey, stops: list[JourneyStop]):
    # Recherche existante (vehicle_uri, service_date)
    stmt = select(Journey).where(Journey.vehicle_uri == j.vehicle_uri, Journey.service_date == j.service_date)
    existing = session.execute(stmt).scalars().first()

    if existing:
        # Mettre à jour les champs principaux
        for attr in ["vehicle_name","from_station_uri","to_station_uri","planned_departure","planned_arrival",
                     "realtime_departure","realtime_arrival","status","direction"]:
            setattr(existing, attr, getattr(j, attr))
        # Remplacer les stops
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

def list_journeys(session: Session, status: str | None = None, limit: int = 100):
    stmt = select(Journey).order_by(Journey.planned_departure.desc()).limit(limit)
    if status in ("running","completed"):
        stmt = select(Journey).where(Journey.status == status).order_by(Journey.planned_departure.desc()).limit(limit)
    return session.execute(stmt).scalars().all()

def get_journey_with_stops(session: Session, journey_id: int):
    j = session.get(Journey, journey_id)
    if not j:
        return None, []
    stops = session.execute(select(JourneyStop).where(JourneyStop.journey_id == journey_id).order_by(JourneyStop.stop_order)).scalars().all()
    return j, stops
