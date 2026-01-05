#!/usr/bin/env python3
"""
Pre-create Kafka topics to avoid initialization errors
"""

import os
import sys
import logging
from pathlib import Path

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from dotenv import load_dotenv

# ===========================
# Environment
# ===========================
dotenv_path = Path(r"C:\API_Integration\Secrets.env")
load_dotenv(dotenv_path=dotenv_path)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "weather_raw")
KAFKA_SCHEDULER_TOPIC = os.getenv("KAFKA_SCHEDULER_TOPIC", "ingestion_triggers")

# ===========================
# Logging
# ===========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("kafka-setup")

# ===========================
# Topic Setup
# ===========================
def create_topics():
    """Create required Kafka topics"""
    admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP)
    
    topics = [
        NewTopic(name=KAFKA_TOPIC, num_partitions=1, replication_factor=1),
        NewTopic(name=KAFKA_SCHEDULER_TOPIC, num_partitions=1, replication_factor=1),
    ]
    
    try:
        result = admin_client.create_topics(new_topics=topics, validate_only=False)
        for topic, future in result.items():
            try:
                future.result()  # Wait for operation to complete
                logger.info(f"✓ Topic '{topic}' created successfully")
            except TopicAlreadyExistsError:
                logger.info(f"✓ Topic '{topic}' already exists")
            except Exception as e:
                logger.error(f"✗ Failed to create topic '{topic}': {e}")
                raise
    finally:
        admin_client.close()

if __name__ == "__main__":
    logger.info(f"Creating Kafka topics on {KAFKA_BOOTSTRAP}...")
    try:
        create_topics()
        logger.info("All topics ready!")
    except Exception as e:
        logger.error(f"Topic setup failed: {e}")
        sys.exit(1)
