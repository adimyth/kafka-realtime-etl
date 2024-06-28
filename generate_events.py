import concurrent.futures
import json
import random
from datetime import datetime, timedelta

import requests

# API endpoint
API_URL = "http://localhost:8000/track"

# Event types and corresponding event names
EVENT_TYPES = {
    "Presentation": [
        "share_presentation",
        "view_presentation",
        "download_presentation",
    ],
    "DPE": ["dpe_click", "dpe_view", "dpe_submit", "dpe_share"],
    "DVC": ["dvc_view", "dvc_update", "dvc_share"],
    "Sync": ["sync_start", "sync_end", "sync_error"],
}

PLATFORMS = ["ios", "android", "web"]
USER_NAMES = ["John Doe", "Jane Smith", "Alice Johnson", "Bob Brown"]


def random_event_data():
    event_type = random.choice(list(EVENT_TYPES.keys()))
    event_name = random.choice(EVENT_TYPES[event_type])
    user_name = random.choice(USER_NAMES)
    platform = random.choice(PLATFORMS)
    triggered_at = (
        datetime.now() - timedelta(minutes=random.randint(0, 59))
    ).isoformat()

    return {
        "event_name": event_name,
        "user_details": {"user_name": user_name},
        "event_details": {"platform": platform},
        "triggered_at": triggered_at,
        "event_type": event_type,
    }


def send_event():
    event_data = random_event_data()
    response = requests.post(API_URL, json=event_data)
    print(
        f"Sent event: {json.dumps(event_data)}, Status Code: {response.status_code}, Response: {response.text}"
    )


def main():
    num_requests = 200  # Number of requests to send
    max_workers = 10  # Number of parallel workers

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(send_event) for _ in range(num_requests)]
        concurrent.futures.wait(futures)


if __name__ == "__main__":
    main()
