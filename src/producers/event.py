import time

from confluent_kafka import KafkaError, KafkaException, Producer
from confluent_kafka.serialization import MessageField, SerializationContext
from loguru import logger

from src.schema_registry.event import event_key_serializer, event_value_serializer
from src.settings import settings

# Producer setup
producer_conf = {
    "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
    "client.id": "event-producer",
}
producer = Producer(producer_conf)


def event_delivery_report(err, msg):
    if err is not None:
        logger.error(f"Failed to deliver message: {str(err)}")
    else:
        logger.info(
            f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}"
        )


class EventProducer:
    @staticmethod
    def produce(
        topic: str,
        key: str,
        value: dict,
        on_delivery: callable = None,
        max_retries: int = 3,
    ):
        """
        Produce an event to the specified Kafka topic.

        Retries the operation if a retriable error occurs with exponential backoff.

        Args:
            topic (str): Kafka topic to produce the event to
            key (str): Key for the event
            value (dict): Event data to be produced
            on_delivery (callable, optional): Callback function for delivery report
            max_retries (int, optional): Maximum number of retries for the operation
        """
        for attempt in range(max_retries):
            try:
                producer.produce(
                    topic=topic,
                    key=event_key_serializer(key),
                    value=event_value_serializer(
                        value, SerializationContext(topic, MessageField.VALUE)
                    ),
                    on_delivery=on_delivery,
                )
                # If produce is successful, break out of the retry loop
                break
            except KafkaException as e:
                kafka_error = e.args[0]
                if kafka_error.code() in [
                    KafkaError.MSG_SIZE_TOO_LARGE,
                    KafkaError._MAX_POLL_EXCEEDED,
                ]:
                    # These are not retriable errors, so we raise immediately
                    logger.error(f"Non-retriable Kafka error: {str(e)}")
                    raise
                if attempt == max_retries - 1:
                    logger.error(
                        f"Failed to produce message after {max_retries} attempts: {str(e)}"
                    )
                    raise
                logger.warning(
                    f"Retriable Kafka error on attempt {attempt + 1}, retrying: {str(e)}"
                )
                time.sleep(2**attempt)
            except ValueError as e:
                logger.error(f"Invalid input: {str(e)}")
                raise
            except Exception as e:
                logger.error(
                    f"Unexpected error while producing record value - {value} to topic - {topic}: {str(e)}"
                )
                raise

        # Flush after successful production or max retries
        producer.flush()

    @staticmethod
    def flush():
        """
        Flush the producer to ensure all messages are delivered.
        """
        producer.flush()
