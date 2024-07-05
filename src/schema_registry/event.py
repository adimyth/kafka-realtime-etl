from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONDeserializer, JSONSerializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    StringDeserializer,
    StringSerializer,
)

from src.schemas.event import event_from_dict, event_to_dict
from src.settings import settings

# Schema Registry setup
schema_registry_conf = {"url": settings.KAFKA_SCHEMA_REGISTRY_URL}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)


event_schema = """
{
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "Event",
    "type": "object",
    "properties": {
        "event_type": {
            "type": "string"
        },
        "event_name": {
            "type": "string"
        },
        "user_details": {
            "type": "object",
            "additionalProperties": {
                "type": ["string", "number", "boolean", "null"]
            }
        },
        "event_details": {
            "type": "object",
            "additionalProperties": {
                "type": ["string", "number", "boolean", "null"]
            }
        },
        "triggered_at": {
            "type": "string"
        }
    },
    "required": [
        "event_type",
        "event_name",
        "user_details",
        "event_details",
        "triggered_at"
    ]
}
"""

# String Serializer - use to serialize the event key before sending it to Kafka (same for producer and consumer)
event_key_serializer = StringSerializer("utf_8")
event_key_deserializer = StringDeserializer("utf_8")

# JSON Value Serializer - used in the producer to serialize Python objects to bytes before sending them to Kafka
event_value_serializer = JSONSerializer(
    schema_str=event_schema,
    schema_registry_client=schema_registry_client,
    to_dict=event_to_dict,
)

# JSON Value Deserializer - used in the consumer to deserialize the event data (stored in bytes in Kafka) from Kafka messages to Python objects
event_value_deserializer = JSONDeserializer(
    schema_str=event_schema,
    schema_registry_client=schema_registry_client,
    from_dict=event_from_dict,
)


def serialize_event(topic, event):
    return event_value_serializer(
        event, SerializationContext(topic, MessageField.VALUE)
    )


def deserialize_event(topic, message):
    return event_value_deserializer(
        message, SerializationContext(topic, MessageField.VALUE)
    )
