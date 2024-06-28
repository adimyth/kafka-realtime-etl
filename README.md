# Kafka Realtime ETL

This project is a simple example of a real-time ETL pipeline using Kafka, Schema Registry, Postgres, and Python.

## Setup
Imagine a scenario where your application is producing events data and you want to create a real-time ETL pipeline to process and store this data in a database. This project is a simple example of how you can achieve this using Kafka, Schema Registry, Postgres, and Python.

PS - You can then create visualizations on top of it to create a real-time analytics dashboard using Apache Superset or any other BI tool.


## Running the application

1️⃣ Create a `.env` file by copying the `.env.dev` file and updating the values as needed.

2️⃣ Start the Kafka, Schema Registry and Postgres containers with the following command:
```bash
docker compose up -d kafka schema-registry postgres
```

3️⃣ Start the producer once the Kafka container is running & the application is initialized:
```bash
docker compose build producer

docker compose up -d producer
```
The application will start at `http://localhost:8080`.

4️⃣ Generate events data - I have created a script to generate events data. It generates events data (by hitting the `/track` api) and sends it to the Kafka topic
```bash
python3 generate_events.py
```

5️⃣ Start the consumer
```bash
docker compose build consumer

docker compose up -d consumer
```

6️⃣ Start the ***correction mechanism*** once the consumer is running:
```bash
docker compose build correction_mechanism

docker compose up -d correction_mechanism
```

7️⃣ Validate that the data is processed & stored in the Postgres database by the consumer:
```bash
# Login to the Postgres container
docker container exec -it postgres "/bin/bash"

# Connect to the Postgres database
psql -U postgres

# Connect to the postgres database
\c postgres

# List the tables
\dt

# Query the table
SELECT COUNT(*) FROM public.processed_events;
SELECT event_type, count(event_type) FROM public.processed_events GROUP BY event_type;
SELECT event_name, count(event_name) FROM public.processed_events GROUP BY event_name;
```
