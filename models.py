from sqlalchemy import JSON, Column, DateTime, Integer, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class ProcessedEvent(Base):
    __tablename__ = "processed_events"

    id = Column(Integer, primary_key=True)
    event_name = Column(String)
    user_details = Column(JSON)
    event_details = Column(JSON)
    triggered_at = Column(DateTime)
    event_type = Column(String)
    processed_at = Column(DateTime)
