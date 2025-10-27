import os
import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import text

# Imports partagés
from shared.database import engine, SessionLocal
from shared.models import Base, Journey, JourneyStop

# Initialisation du logger
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)

# Création des tables si elles n'existent pas
Base.metadata.create_all(bind=engine)

# Instance principale FastAPI
app = FastAPI(
    title="SNCB Backend API",
    description="Backend FastAPI pour la collecte et l’exposition des trajets SNCB/iRail",
    version="1.0.0"
)

# Middleware CORS (autorise ton domaine frontend)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tu peux restreindre à ["https://sncb.terminalcommun.be"]
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Dépendance DB
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# --- Routes de base ---

@app.get("/healthz")
def healthz():
    """Route de test simple pour vérifier que le backend est en vie."""
    return {"ok": True}

@app.get("/")
def root():
    """Page racine (redirection ou message d’accueil)."""
    return {"message": "Bienvenue sur l’API SNCB Backend"}

@app.get("/journeys")
def get_journeys(limit: int = 10):
    """Retourne les derniers trajets connus."""
    with SessionLocal() as db:
        rows = db.query(Journey).order_by(Journey.id.desc()).limit(limit).all()
        return [j.as_dict() for j in rows]

@app.get("/journeys/{journey_id}/stops")
def get_stops(journey_id: int):
    """Retourne les arrêts d’un trajet donné."""
    with SessionLocal() as db:
        stops = (
            db.query(JourneyStop)
            .filter(JourneyStop.journey_id == journey_id)
            .order_by(JourneyStop.stop_order)
            .all()
        )
        return [s.as_dict() for s in stops]

@app.get("/dbping")
def db_ping():
    """Vérifie la connexion à la base MariaDB."""
    try:
        with SessionLocal() as db:
            db.execute(text("SELECT 1"))
        return {"db": "ok"}
    except Exception as e:
        logger.error(f"Erreur DB: {e}")
        return {"db": "error", "detail": str(e)}

# --- Démarrage (pour uvicorn) ---

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "backend.main:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", 8000)),
        reload=False
    )
