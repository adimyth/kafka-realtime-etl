import asyncio
import os
from collections import defaultdict
from datetime import datetime

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from confluent_kafka import Consumer
from loguru import logger

from src.schema_registry.event import deserialize_event
from src.settings import settings


class MultiTopicSummaryConsumer:
    def __init__(self):
        self.consumer = Consumer(
            {
                "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
                "group.id": "multi_topic_summary_consumer",
                "auto.offset.reset": "earliest",
            }
        )
        self.topics = [
            "event_purchase",
            "event_order",
            "event_delivery",
        ]
        self.consumer.subscribe(self.topics)

        self.purchase_summary = {
            event: {"total_items": 0, "total_revenue": 0.0}
            for event in ["added_to_cart", "checkout_initiated", "payment_successful"]
        }
        self.placing_order_summary = {
            event: {"total_orders": 0, "total_value": 0.0}
            for event in ["order_placed", "order_processed", "order_shipped"]
        }
        self.transit_delivery_summary = {
            "order_shipped": {
                "items_in_transit": 0,
                "carrier_allocation": defaultdict(int),
            },
            "order_delivered": {
                "items_delivered": 0,
                "carrier_allocation": defaultdict(int),
            },
        }
        self.chart_dir = "/app/event_summary_charts"
        os.makedirs(self.chart_dir, exist_ok=True)

    def process_purchase_event(self, event):
        event_name = event.event_name
        event_details = event.event_details
        if event_name in self.purchase_summary:
            self.purchase_summary[event_name]["total_items"] += 1
            self.purchase_summary[event_name]["total_revenue"] += float(
                event_details.get("price", 0)
            )

    def process_placing_order_event(self, event):
        event_name = event.event_name
        event_details = event.event_details
        if event_name in self.placing_order_summary:
            self.placing_order_summary[event_name]["total_orders"] += 1
            self.placing_order_summary[event_name]["total_value"] += float(
                event_details.get("order_value", 0)
            )

    def process_transit_delivery_event(self, event):
        event_name = event.event_name
        event_details = event.event_details
        if event_name in self.transit_delivery_summary:
            if event_name == "order_shipped":
                self.transit_delivery_summary[event_name]["items_in_transit"] += 1
            elif event_name == "order_delivered":
                self.transit_delivery_summary[event_name]["items_delivered"] += 1

            carrier = event_details.get("carrier")
            if carrier:
                self.transit_delivery_summary[event_name]["carrier_allocation"][
                    carrier
                ] += 1

    def create_purchase_chart(self):
        df = pd.DataFrame(self.purchase_summary).T.reset_index()
        df.columns = ["Event", "Total Items", "Total Revenue"]

        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 16))

        sns.barplot(
            x="Event", y="Total Items", data=df, ax=ax1, hue="Event", legend=False
        )
        ax1.set_title("Total Items by Purchase Event", fontsize=16)
        ax1.set_ylabel("Number of Items", fontsize=12)
        ax1.tick_params(axis="x", rotation=45)

        sns.barplot(
            x="Event", y="Total Revenue", data=df, ax=ax2, hue="Event", legend=False
        )
        ax2.set_title("Total Revenue by Purchase Event", fontsize=16)
        ax2.set_ylabel("Revenue ($)", fontsize=12)
        ax2.tick_params(axis="x", rotation=45)

        for ax in [ax1, ax2]:
            for i, v in enumerate(ax.containers):
                ax.bar_label(v, fmt="%.2f", fontsize=10)

        plt.tight_layout()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        plt.savefig(
            f"{self.chart_dir}/purchase_summary_{timestamp}.png",
            dpi=300,
            bbox_inches="tight",
        )
        plt.close()

    def create_placing_order_chart(self):
        df = pd.DataFrame(self.placing_order_summary).T.reset_index()
        df.columns = ["Event", "Total Orders", "Total Value"]
        df["Average Value"] = df["Total Value"] / df["Total Orders"]

        fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(12, 20))

        sns.barplot(
            x="Event", y="Total Orders", data=df, ax=ax1, hue="Event", legend=False
        )
        ax1.set_title("Total Orders by Placing Order Event", fontsize=16)
        ax1.set_ylabel("Number of Orders", fontsize=12)
        ax1.tick_params(axis="x", rotation=45)

        sns.barplot(
            x="Event", y="Total Value", data=df, ax=ax2, hue="Event", legend=False
        )
        ax2.set_title("Total Order Value by Placing Order Event", fontsize=16)
        ax2.set_ylabel("Order Value ($)", fontsize=12)
        ax2.tick_params(axis="x", rotation=45)

        sns.barplot(
            x="Event", y="Average Value", data=df, ax=ax3, hue="Event", legend=False
        )
        ax3.set_title("Average Order Value by Placing Order Event", fontsize=16)
        ax3.set_ylabel("Average Value ($)", fontsize=12)
        ax3.tick_params(axis="x", rotation=45)

        for ax in [ax1, ax2, ax3]:
            for i, v in enumerate(ax.containers):
                ax.bar_label(v, fmt="%.2f", fontsize=10)

        plt.tight_layout()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        plt.savefig(
            f"{self.chart_dir}/placing_order_summary_{timestamp}.png",
            dpi=300,
            bbox_inches="tight",
        )
        plt.close()

    def create_transit_delivery_chart(self):
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 16))

        # Items in Transit vs Delivered
        transit_data = {
            "Status": ["In Transit", "Delivered"],
            "Count": [
                self.transit_delivery_summary["order_shipped"]["items_in_transit"],
                self.transit_delivery_summary["order_delivered"]["items_delivered"],
            ],
        }
        transit_df = pd.DataFrame(transit_data)
        sns.barplot(
            x="Status", y="Count", data=transit_df, ax=ax1, hue="Status", legend=False
        )
        ax1.set_title("Items in Transit vs Delivered", fontsize=16)
        ax1.set_ylabel("Number of Items", fontsize=12)
        for i, v in enumerate(ax1.containers):
            ax1.bar_label(v, fmt="%.0f", fontsize=10)

        # Carrier Allocation
        carrier_data = defaultdict(int)
        for status in ["order_shipped", "order_delivered"]:
            for carrier, count in self.transit_delivery_summary[status][
                "carrier_allocation"
            ].items():
                carrier_data[carrier] += count

        carrier_df = pd.DataFrame(
            list(carrier_data.items()), columns=["Carrier", "Count"]
        )
        carrier_df = carrier_df.sort_values("Count", ascending=False)

        sns.barplot(
            x="Count",
            y="Carrier",
            data=carrier_df,
            ax=ax2,
            hue="Carrier",
            legend=False,
            orient="h",
        )
        ax2.set_title("Carrier Allocation", fontsize=16)
        ax2.set_xlabel("Number of Items", fontsize=12)
        for i, v in enumerate(ax2.containers):
            ax2.bar_label(v, fmt="%.0f", fontsize=10)

        plt.tight_layout()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        plt.savefig(
            f"{self.chart_dir}/transit_delivery_summary_{timestamp}.png",
            dpi=300,
            bbox_inches="tight",
        )
        plt.close()

    def create_charts(self):
        sns.set_style("whitegrid")
        plt.rcParams["font.size"] = 12
        plt.rcParams["axes.titlesize"] = 16
        plt.rcParams["axes.labelsize"] = 14

        self.create_purchase_chart()
        self.create_placing_order_chart()
        self.create_transit_delivery_chart()
        logger.info(f"Enhanced charts created and saved in {self.chart_dir}")

    async def consume_events(self):
        message_count = 0
        last_chart_time = datetime.now()

        while True:
            try:
                messages = self.consumer.consume(num_messages=100, timeout=1.0)

                for msg in messages:
                    if msg is None:
                        continue
                    if msg.error():
                        logger.error(f"Consumer error: {msg.error()}")
                        continue

                    topic = msg.topic()
                    event = deserialize_event(topic, msg.value())

                    if topic == "event_purchase":
                        self.process_purchase_event(event)
                    elif topic == "event_order":
                        self.process_placing_order_event(event)
                    elif topic == "event_delivery":
                        self.process_transit_delivery_event(event)

                    message_count += 1

                # Generate charts every 5 minutes or every 1000 messages, whichever comes first
                current_time = datetime.now()
                if (
                    current_time - last_chart_time
                ).total_seconds() >= 300 or message_count >= 1000:
                    self.create_charts()
                    last_chart_time = current_time
                    logger.info(f"Processed {message_count} messages. Charts updated.")
                    message_count = 0

            except Exception as e:
                logger.error(f"Unexpected error: {str(e)}")

            # Small delay to prevent tight looping
            await asyncio.sleep(0.1)

    def close(self):
        self.consumer.close()


async def main():
    consumer = MultiTopicSummaryConsumer()
    try:
        await consumer.consume_events()
    finally:
        consumer.close()


if __name__ == "__main__":
    asyncio.run(main())
