# --- worker/models.py ---
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy import String, Integer, BigInteger, Enum, DateTime, Date, ForeignKey, Boolean

class Base(DeclarativeBase):
    pass

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

    stops: Mapped[list["JourneyStop"]] = relationship("JourneyStop", back_populates="journey", cascade="all, delete-orphan")

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

    journey: Mapped["Journey"] = relationship("Journey", back_populates="stops")
