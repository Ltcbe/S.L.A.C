# --- backend/main.py ---
import os
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import text
from datetime import datetime

from database import SessionLocal, engine
from models import Base, Journey, JourneyStop
from crud import list_journeys, get_journey_with_stops

Base.metadata.create_all(bind=engine)

app = FastAPI(title="SNCB Slac API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

class JourneyOut(BaseModel):
    id: int
    vehicle_name: str
    vehicle_uri: str
    service_date: datetime
    planned_departure: datetime
    planned_arrival: datetime
    status: str
    class Config:
        from_attributes = True

class StopOut(BaseModel):
    stop_order: int
    station_name: str
    planned_arrival: datetime | None
    planned_departure: datetime | None
    realtime_arrival: datetime | None
    realtime_departure: datetime | None
    platform: str | None
    arrived: bool
    left: bool
    is_extra_stop: bool
    arrival_canceled: bool
    departure_canceled: bool
    class Config:
        from_attributes = True

@app.get("/healthz")
def healthz():
    with SessionLocal() as s:
        s.execute(text("SELECT 1"))
    return {"ok": True}

@app.get("/trains", response_model=list[JourneyOut])
def api_list_trains(status: str = Query(None, pattern="^(running|completed)$")):
    with SessionLocal() as s:
        items = list_journeys(s, status=status, limit=200)
        return items

@app.get("/trains/{journey_id}")
def api_get_train(journey_id: int):
    with SessionLocal() as s:
        j, stops = get_journey_with_stops(s, journey_id)
        if not j:
            raise HTTPException(404, "journey not found")
        return {
            "journey": JourneyOut.model_validate(j).model_dump(),
            "stops": [StopOut.model_validate(x).model_dump() for x in stops],
        }
