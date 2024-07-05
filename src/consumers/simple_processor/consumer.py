import asyncio
from datetime import datetime
from typing import List

from confluent_kafka import Consumer, Producer
from loguru import logger

from src.db.session import engine, get_db_session_cm
from src.models.processed_event import ProcessedEvent
from src.schema_registry.event import (
    deserialize_event,
    event_key_serializer,
    serialize_event,
)
from src.schemas.event import Event
from src.settings import settings


class SimplifiedETLConsumer:
    def __init__(self, event_type: str):
        self.event_type = event_type
        self.consumer = Consumer(
            {
                "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
                "group.id": f"{event_type}_consumer_group",
                "auto.offset.reset": "earliest",
            }
        )
        self.producer = Producer(
            {
                "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
            }
        )
        self.topic = f"event_{event_type}"
        self.dlq_topic = f"dlq_{event_type}"
        self.consumer.subscribe([self.topic])

    async def process_events(self, events: List[Event]) -> None:
        """
        Process and store the events in the database.

        This is where the ETL logic would be implemented.
        """
        async with get_db_session_cm() as session:
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

                session.add_all(processed_events)
                await session.commit()
                logger.info(f"Processed and stored {len(processed_events)} events")
            except Exception as e:
                logger.error(f"Error processing events: {str(e)}")
                await session.rollback()
                raise

    def send_to_dlq(self, event: Event):
        try:
            self.producer.produce(
                self.dlq_topic,
                key=event_key_serializer(event.event_name, None),
                value=serialize_event(self.dlq_topic, event),
                on_delivery=self.dlq_delivery_report,
            )
            self.producer.flush()
        except Exception as e:
            logger.error(f"Error sending event to DLQ: {str(e)}")

    def dlq_delivery_report(self, err, msg):
        if err is not None:
            logger.error(f"Failed to deliver message to DLQ: {str(err)}")
        else:
            logger.info(
                f"Message delivered to DLQ {msg.topic()} [{msg.partition()}] at offset {msg.offset()}"
            )

    async def consume_events(self):
        batch_size = 100
        batch = []
        max_retries = 3

        while True:
            try:
                # Fetch messages from Kafka in batches
                messages = self.consumer.consume(num_messages=batch_size, timeout=1.0)

                for msg in messages:
                    if msg.error():
                        logger.error(f"Consumer error: {msg.error()}")
                        continue

                    value = deserialize_event(self.topic, msg.value())
                    batch.append(value)

                if batch:
                    for retry in range(max_retries):
                        try:
                            await self.process_events(batch)
                            self.consumer.commit()
                            batch = []
                            break
                        except Exception as e:
                            logger.error(f"Error processing batch, retrying: {str(e)}")
                            await asyncio.sleep(2**retry)
                    else:
                        # If all retries fail, send to Dead-Letter Queue
                        for event in batch:
                            self.send_to_dlq(event)
                        self.consumer.commit()
                        batch = []

            except Exception as e:
                logger.error(f"Unexpected error: {str(e)}")
                await asyncio.sleep(5)

    def run(self):
        while True:
            try:
                asyncio.run(self.consume_events())
            except KeyboardInterrupt:
                logger.info("Stopping consumer...")
                break
            except Exception as e:
                logger.error(f"Consumer loop error: {str(e)}")
                logger.info("Restarting consumer loop in 10 seconds...")
                asyncio.sleep(10)


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 2:
        print("Usage: python simplified_consumer.py <event_type>")
        sys.exit(1)

    event_type = sys.argv[1]
    consumer = SimplifiedETLConsumer(event_type)
    consumer.run()
