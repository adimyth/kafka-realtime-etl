from typing import Any, Dict

from pydantic import BaseModel


class Event(BaseModel):
    event_name: str
    user_details: Dict[str, Any]
    event_details: Dict[str, Any]
    triggered_at: str
    event_type: str

    def __getitem__(self, key):
        return getattr(self, key)


def event_to_dict(event: Event, ctx) -> Dict:
    return event.model_dump()


def event_from_dict(event_dict: Dict, ctx) -> Event:
    return Event(**event_dict)
