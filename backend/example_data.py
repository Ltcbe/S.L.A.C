# --- backend/example_data.py ---
from datetime import datetime, date, timedelta
from sqlalchemy import delete
from shared.database import SessionLocal, engine
from shared.models import Base, Journey, JourneyStop

Base.metadata.create_all(bind=engine)

def seed_example():
    with SessionLocal() as s:
        j = Journey(
            vehicle_uri="http://irail.be/vehicle/IC3033",
            vehicle_name="IC3033",
            service_date=date.today(),
            from_station_uri="http://irail.be/stations/NMBS/008892007",  # Tournai
            to_station_uri="http://irail.be/stations/NMBS/008812005",    # Bruxelles-Central
            planned_departure=datetime.now().replace(minute=0, second=0, microsecond=0),
            planned_arrival=datetime.now().replace(minute=0, second=0, microsecond=0) + timedelta(hours=1, minutes=5),
            realtime_departure=None,
            realtime_arrival=None,
            status="completed",
            direction="Bruxelles-Central",
        )
        s.add(j); s.flush()

        stops = [
            JourneyStop(
                journey_id=j.id, stop_order=1,
                station_uri=j.from_station_uri, station_name="Tournai",
                planned_departure=j.planned_departure, arrived=False, left=True
            ),
            JourneyStop(
                journey_id=j.id, stop_order=2,
                station_uri="http://irail.be/stations/NMBS/008821008", station_name="Mons",
                planned_arrival=j.planned_departure + timedelta(minutes=23),
                planned_departure=j.planned_departure + timedelta(minutes=25),
                arrived=True, left=True
            ),
            JourneyStop(
                journey_id=j.id, stop_order=3,
                station_uri=j.to_station_uri, station_name="Bruxelles-Central",
                planned_arrival=j.planned_arrival, arrived=True, left=False
            ),
        ]
        s.add_all(stops); s.commit()
        print("✅ Example data inserted")

def wipe_example():
    with SessionLocal() as s:
        s.execute(delete(JourneyStop))
        s.execute(delete(Journey))
        s.commit()
        print("🧹 Example data wiped")

if __name__ == "__main__":
    seed_example()
