import json
import time
from datetime import datetime
from typing import List

from confluent_kafka import DeserializingConsumer, SerializingProducer
from confluent_kafka.serialization import StringDeserializer, StringSerializer
from loguru import logger
from models import Base, ProcessedEvent
from schema import Event
from schema_registry import event_json_deserializer
from settings import settings
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Event types
EVENT_TYPES = ["DVC", "DPE", "Presentation", "Sync"]

# Database engine setup
database_url = f"postgresql://{settings.POSTGRES_USER}:{settings.POSTGRES_PASSWORD.get_secret_value()}@{settings.POSTGRES_HOST}:{settings.POSTGRES_PORT}/{settings.POSTGRES_DB}"
engine = create_engine(database_url)
Session = sessionmaker(bind=engine)

# Kafka consumer setup
consumer_conf = {
    "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
    "group.id": settings.KAFKA_CONSUMER_GROUP_ID,
    # 🔥 If the consumer group is new or has no committed offsets, it will start reading from the earliest available message.
    "auto.offset.reset": "earliest",
    # 🔥 If the consumer container goes down and restarts, it will resume from the last committed offset, thanks to manual commits after successful processing
    "enable.auto.commit": False,
    "key.deserializer": StringDeserializer("utf_8"),
    "value.deserializer": event_json_deserializer,
}
consumer = DeserializingConsumer(consumer_conf)

# Dead-Letter Queue setup
dlq_producer = SerializingProducer(
    {
        "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
        "key.serializer": StringSerializer("utf_8"),
        "value.serializer": StringSerializer("utf_8"),
    }
)


def process_events(events: List[Event]) -> None:
    """Process a batch of events and store them in the OLAP database."""
    session = Session()
    try:
        processed_events = []
        for event in events:
            processed_event = ProcessedEvent(
                event_name=event.event_name,
                user_details=event.user_details,
                event_details=event.event_details,
                triggered_at=datetime.fromisoformat(event.triggered_at),
                event_type=event.event_type,
                processed_at=datetime.now(),
            )
            processed_events.append(processed_event)

        session.bulk_save_objects(processed_events)
        session.commit()
        logger.info(f"Processed and stored {len(processed_events)} events")
    except Exception as e:
        logger.error(f"Error processing events: {str(e)}")
        session.rollback()
        # Send failed events to Dead-Letter Queue
        for event in events:
            dlq_producer.produce(
                settings.DLQ_TOPIC,
                key=event.event_name,
                value=event.model_dump_json(),
                on_delivery=dlq_delivery_report,
            )
        dlq_producer.flush()
    finally:
        session.close()


def dlq_delivery_report(err, msg):
    if err is not None:
        logger.error(f"Failed to deliver message to DLQ: {str(err)}")
    else:
        logger.info(
            f"Message delivered to DLQ {msg.topic()} [{msg.partition()}] at offset {msg.offset()}"
        )


def exponential_backoff(retries):
    """Calculate exponential backoff time for retries."""
    return min(2**retries, 60)


def consume_events():
    """Consume events from Kafka topics and process them in batches."""
    topics = [f"event_{event_type}" for event_type in EVENT_TYPES]
    consumer.subscribe(topics)

    batch_size = 100
    batch = []
    max_retries = 3

    try:
        while True:
            try:
                logger.info("Polling for messages...")
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    logger.error(f"Consumer error: {msg.error()}")
                    continue

                logger.info(f"Received message: {msg.key()}: {msg.value()}")
                batch.append(msg.value())
                if len(batch) >= batch_size:
                    for retry in range(max_retries):
                        try:
                            process_events(batch)
                            consumer.commit()
                            batch = []
                            break
                        except Exception as e:
                            logger.error(f"Error processing batch, retrying: {str(e)}")
                            time.sleep(exponential_backoff(retry))
                    else:
                        # If all retries fail, send to Dead-Letter Queue
                        for event in batch:
                            dlq_producer.produce(
                                settings.DLQ_TOPIC,
                                key=event.event_name,
                                value=event.model_dump_json(),
                                on_delivery=dlq_delivery_report,
                            )
                        dlq_producer.flush()
                        consumer.commit()
                        batch = []

            except Exception as e:
                logger.error(f"Unexpected error: {str(e)}")
                # Implement retry logic or move to dead-letter queue
    except KeyboardInterrupt:
        logger.info("Stopping consumer...")
    finally:
        consumer.close()


if __name__ == "__main__":
    Base.metadata.create_all(engine)
    consume_events()
