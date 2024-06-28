from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from fastapi import FastAPI, HTTPException
from loguru import logger
from schema import Event
from schema_registry import event_json_serializer
from settings import settings

# Event types
EVENT_TYPES = ["DVC", "DPE", "Presentation", "Sync"]

# Kafka producer setup with Schema Registry
producer_conf = {
    "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
    "key.serializer": StringSerializer("utf_8"),
    "value.serializer": event_json_serializer,
    # TODO: Figure this out
    # 🔥 Setting "exactly-once" delivery guarantees as well as ordering of messages in a partition. However, this is causing the producer to not load - Failed to acquire idempotence PID from broker kafka:9092/bootstrap: Broker: Coordinator load in progress: retrying
    # "enable.idempotence": True,
    # "acks": "all",
    # "retries": 5,
    # "max.in.flight.requests.per.connection": 1,
}
producer = SerializingProducer(producer_conf)

# Setting up a FastAPI app
app = FastAPI()


@app.post("/track")
async def track_event(event: Event):
    try:
        # Send to Kafka with Schema Registry
        if event.event_type in EVENT_TYPES:
            topic = f"event_{event.event_type}"
            producer.produce(
                topic=topic,
                key=str(event.event_name),
                value=event.dict(),
                on_delivery=delivery_report,
            )
            producer.flush()
        else:
            logger.warning(f"Invalid event type: {event.event_type}")
            raise HTTPException(status_code=400, detail="Invalid event type")

        return {"message": "Event tracked successfully"}
    except Exception as e:
        logger.error(f"Error tracking event: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Failed to deliver message: {str(err)}")
    else:
        logger.info(
            f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}"
        )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
