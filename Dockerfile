# Use Python 3.11 slim as base
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies needed for building Python packages
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        gcc \
        g++ \
        libffi-dev \
        libssl-dev \
        python3-dev \
        libpq-dev \
        curl \
        wget \
        unzip \
        git && \
    rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies without cache
RUN pip install --upgrade pip setuptools wheel && \
    pip install --no-cache-dir -r requirements.txt

# Copy the rest of the app
COPY . .

# Expose port if needed
EXPOSE 8000

# Set default command
CMD ["python", "main.py"]
