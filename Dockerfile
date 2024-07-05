FROM python:3.10.8-slim-buster

# Set working directory
RUN mkdir -p /app
WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
ENV LOG_LEVEL INFO

# Install system dependencies
RUN apt-get update && apt-get install -y \
    libfreetype6-dev \
    libpng-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install dependencies
RUN pip install --upgrade pip
COPY src/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project
COPY .env .
COPY src /app/src

COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

# Run the application
ENTRYPOINT ["/app/entrypoint.sh"]