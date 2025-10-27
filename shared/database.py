# --- shared/database.py ---
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

DB_DSN = os.getenv("DB_DSN", "mysql+pymysql://app:change_me@db:3306/sncbslac")

engine = create_engine(
    DB_DSN,
    pool_pre_ping=True,
    pool_recycle=3600,
    pool_size=5,
    max_overflow=10,
    future=True,
)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
