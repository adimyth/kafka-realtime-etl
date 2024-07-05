from fastapi import FastAPI, HTTPException
from loguru import logger

from src.db.session import create_tables
from src.producers.event import EventProducer, event_delivery_report
from src.schemas.event import Event
from src.models.processed_event import ProcessedEvent

app = FastAPI()


@app.on_event("startup")
async def startup_event():
    await create_tables()


@app.post("/track")
async def track_event(event: Event):
    """
    Track an event and produce it to the corresponding Kafka topic.

    Args:
        event (Event): Event object to be tracked

    Returns:
        dict: Response message
    """
    try:
        topic = f"event_{event.event_type}"
        EventProducer.produce(
            topic=topic,
            key=str(event.event_name),
            value=event,
            on_delivery=event_delivery_report,
        )
        return {"message": "Event tracked successfully"}
    except Exception as e:
        logger.error(f"Error tracking event: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")
