from kafka import KafkaConsumer
from pymongo import MongoClient, GEOSPHERE

import json

from configuration import *

consumer = None
mongo_client = None

def deserialize(message):
    try:
        j = json.loads(message)
        return j
    except Excecption:
        return {}


def get_consumer():
    """
    Kafka consumer getter
    """
    global consumer
    if consumer is None:
        consumer = KafkaConsumer(RSVPS_TOPIC_NAME,
                         bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                         auto_offset_reset=KAFKA_CONSUMER_AUTO_OFFSET_RESET,
                         enable_auto_commit=KAFKA_CONSUMER_ENABLE_AUTO_COMMIT,
                         value_deserializer=deserialize, # Deserialize as JSON
                         consumer_timeout_ms=KAFKA_CONSUMER_TIMEOUT_MS)
    return consumer

def get_mongo_client():
    """
    Mongo client getter
    """
    global mongo_client
    if mongo_client is None:
        mongo_client = MongoClient(host=MONGO_URL, port=MONGO_PORT, username=MONGO_USER, password=MONGO_PASS, document_class=dict)
    return mongo_client

def get_rsvps_database():
    return get_mongo_client().rsvps


def create_location_index():
    """
    Create location index for the groups collection if it doesn't exists
    """
    get_rsvps_database().groups.create_index([("location", GEOSPHERE)])
