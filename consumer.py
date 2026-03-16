import json
from confluent_kafka import Consumer, Producer
import database
from logger import log_event
import logic

consumer_config = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "hunter_group",
    "auto.offset.reset": "earliest"
}

consumer = Consumer(consumer_config)
consumer.subscribe(['intel', 'attack', 'damage'])

producer_dlq = Producer({"bootstrap.servers": "localhost:9092"})

def handle_intel(data):
    is_valid = logic.validate_intel(data)
    if not is_valid:
        producer_dlq.produce('intel_signals_dlq', json.dumps(data).encode('utf-8'))
        log_event('ERROR', f"invalid intel!!!")
        return
    ent_id = data["entity_id"]
    new_lat = data.get('reported_lat')
    new_lon = data.get('reported_lon')

    previous_data = database.get_target(ent_id)

    if previous_data:
        distance = logic.calculate_dist(previous_data['lat'], previous_data['lon'], new_lat, new_lon)
    else:
        distance = 0.0
    target_info = {"lat": new_lat,
                   "lon": new_lon,
                   "priority": data.get('priority_level', 99),
                   "last_distance": distance}
    
    database.upsert_target(ent_id, target_info)
    log_event('INFO', f"target {ent_id} updated in DB")

print("Waiting for messages from Kafka...")
    


