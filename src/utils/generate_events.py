import random
from datetime import datetime, timedelta
import requests
from collections import Counter

API_URL = "http://localhost:8000/track"


def generate_events(event_type, count):
    events = []
    event_counter = Counter()
    value_counter = Counter()
    carrier_counter = Counter()

    for i in range(count):
        if event_type == "purchase":
            event_name = random.choice(
                ["added_to_cart", "checkout_initiated", "payment_successful"]
            )
            price = round(random.uniform(10, 100), 2)
            event_details = {
                "item_id": i + 1,
                "item_name": f"Item_{i+1}",
                "price": price,
            }
            event_counter[event_name] += 1
            value_counter[event_name] += price

        elif event_type == "order":
            event_name = random.choice(
                ["order_placed", "order_processed", "order_shipped"]
            )
            order_value = round(random.uniform(50, 500), 2)
            event_details = {"order_id": i + 1, "order_value": order_value}
            event_counter[event_name] += 1
            value_counter[event_name] += order_value

        elif event_type == "delivery":
            event_name = random.choice(["order_shipped", "order_delivered"])
            carrier = random.choice(["FedEx", "UPS", "DHL", "USPS", "Amazon Logistics"])
            event_details = {"tracking_id": i + 1, "carrier": carrier}
            event_counter[event_name] += 1
            carrier_counter[carrier] += 1

        event = {
            "event_name": event_name,
            "event_type": event_type,
            "triggered_at": (datetime.now() - timedelta(minutes=i)).isoformat(),
            "user_details": {"user_id": i + 1},
            "event_details": event_details,
        }
        events.append(event)

    return events, event_counter, value_counter, carrier_counter


def send_events(events):
    for event in events:
        requests.post(API_URL, json=event)
    print(f"Sent {len(events)} {events[0]['event_type']} events.")


def print_summary(event_type, event_counter, value_counter, carrier_counter):
    print(f"\nSummary for {event_type} events:")
    total_events = sum(event_counter.values())
    print(f"Total number of events: {total_events}")

    if event_type in ["purchase", "order"]:
        total_value = sum(value_counter.values())
        value_type = "revenue" if event_type == "purchase" else "order value"
        print(f"Total {value_type}: ${total_value:.2f}")

        for event_name in event_counter:
            count = event_counter[event_name]
            value = value_counter[event_name]
            print(f"  {event_name}:")
            print(f"    Count: {count}")
            print(f"    {value_type.capitalize()}: ${value:.2f}")
            if event_type == "order":
                avg_value = value / count if count > 0 else 0
                print(f"    Average Order Value: ${avg_value:.2f}")

    elif event_type == "delivery":
        print(f"Items in transit: {event_counter['order_shipped']}")
        print(f"Items delivered: {event_counter['order_delivered']}")
        print("Carrier allocation:")
        for carrier, count in carrier_counter.most_common():
            print(f"  {carrier}: {count}")


if __name__ == "__main__":
    for event_type in ["purchase", "order", "delivery"]:
        events, event_counter, value_counter, carrier_counter = generate_events(
            event_type, 200
        )
        send_events(events)
        print_summary(event_type, event_counter, value_counter, carrier_counter)
    print("\nAll events sent successfully!")
