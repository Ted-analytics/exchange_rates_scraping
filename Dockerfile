FROM python:3.9-slim

# Install system dependencies and Chrome
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    unzip \
    chromium \
    chromium-driver \
    && rm -rf /var/lib/apt/lists/*

# Set Chrome environment variables
ENV CHROME_BIN=/usr/bin/chromium \
    CHROME_PATH=/usr/lib/chromium/chromium \
    CHROMEDRIVER_PATH=/usr/bin/chromedriver

# Create and set working directory
WORKDIR /opt/dagster/app

# Create dagster home directory
RUN mkdir -p /opt/dagster/dagster_home

# Set environment variable for Dagster
ENV DAGSTER_HOME=/opt/dagster/dagster_home

# Copy project files
COPY setup.py .
COPY exchange_rate_pipeline ./exchange_rate_pipeline/

# Install Python dependencies
RUN pip install -e .

# Create data directory for IO managers
RUN mkdir -p /tmp/exchange_rates /tmp/exchange_rates/db_state

# Command to run when container starts
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "3030", "-f", "/opt/dagster/app/exchange_rate_pipeline/__init__.py"]