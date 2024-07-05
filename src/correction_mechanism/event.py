import asyncio

from confluent_kafka import Consumer, Producer
from loguru import logger

from src.schema_registry.event import (
    deserialize_event,
    event_key_serializer,
    serialize_event,
)
from src.settings import settings


class CorrectionMechanism:
    def __init__(self):
        self.dlq_consumer = Consumer(
            {
                "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
                "group.id": "dlq_processor",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        self.dlq_consumer.subscribe([settings.DLQ_TOPIC])

        self.producer = Producer(
            {
                "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
            }
        )

    async def process_dlq_event(self, event):
        """Process a single event from the DLQ."""
        try:
            # Deserialize the event
            deserialized_event = deserialize_event(settings.DLQ_TOPIC, event)

            # Determine the original topic
            original_topic = f"event_{deserialized_event.event_type}"

            # Serialize the event for the original topic
            serialized_event = serialize_event(original_topic, deserialized_event)

            # TODO: Add any event fixing logic here

            # Push the event back to the original topic for reprocessing
            self.producer.produce(
                topic=original_topic,
                key=event_key_serializer(deserialized_event.event_name),
                value=serialized_event,
                on_delivery=self.reprocess_delivery_report,
            )
            self.producer.flush()

            logger.info(
                f"Successfully reprocessed event: {deserialized_event.event_name}"
            )
            return True
        except Exception as e:
            logger.error(f"Failed to process DLQ event: {str(e)}")
            return False

    def reprocess_delivery_report(self, err, msg):
        if err is not None:
            logger.error(f"Failed to reprocess event: {str(err)}")
        else:
            logger.info(
                f"Event reprocessed to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}"
            )

    async def run(self):
        while True:
            try:
                msg = self.dlq_consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    logger.error(f"DLQ Consumer error: {msg.error()}")
                    continue

                if await self.process_dlq_event(msg.value()):
                    self.dlq_consumer.commit()
            except Exception as e:
                logger.error(f"Unexpected error in correction mechanism: {str(e)}")

            # Sleep for the specified interval
            await asyncio.sleep(settings.CORRECTION_INTERVAL)

    def close(self):
        self.dlq_consumer.close()
        self.producer.flush()


async def main():
    correction_mechanism = CorrectionMechanism()
    try:
        await correction_mechanism.run()
    finally:
        correction_mechanism.close()


if __name__ == "__main__":
    asyncio.run(main())
