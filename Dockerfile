# Use official Python slim image
FROM python:3.13-slim

# Set working directory
WORKDIR /app

# Copy requirements.txt and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the app
COPY . .

# Expose any ports if needed (optional)
# EXPOSE 8080

# Run the ingestion script
CMD ["python", "Api_Ingest.py"]
