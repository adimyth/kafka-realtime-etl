from pydantic import BaseModel
from typing import Dict


class Event(BaseModel):
    event_name: str
    user_details: Dict[str, str]
    event_details: Dict[str, str]
    triggered_at: str
    event_type: str
