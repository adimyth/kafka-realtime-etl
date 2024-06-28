from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONDeserializer, JSONSerializer
from schema import Event
from settings import settings

# Schema Registry setup
schema_registry_conf = {"url": settings.KAFKA_SCHEMA_REGISTRY_URL}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

event_schema = """
{
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "Event",
    "type": "object",
    "properties": {
        "event_name": {
            "type": "string"
        },
        "user_details": {
            "type": "object",
            "additionalProperties": {
                "type": "string"
            }
        },
        "event_details": {
            "type": "object",
            "additionalProperties": {
                "type": "string"
            }
        },
        "triggered_at": {
            "type": "string"
        },
        "event_type": {
            "type": "string"
        }
    },
    "required": ["event_name", "triggered_at", "event_type"]
}
"""


# Define a function to convert dictionary to Event object
def from_dict(data, ctx):
    return Event(**data)


# Create JSON serializer and deserializer
event_json_serializer = JSONSerializer(
    schema_str=event_schema, schema_registry_client=schema_registry_client
)
event_json_deserializer = JSONDeserializer(
    schema_str=event_schema,
    schema_registry_client=schema_registry_client,
    from_dict=from_dict,
)
