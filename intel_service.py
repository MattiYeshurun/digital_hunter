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
consumer.subscribe(['intel'])

producer_dlq = Producer({"bootstrap.servers": "localhost:9092"})

def process_intel():
    print("Intel Service is active")
    while True:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue
        data = json.loads(msg.value().decode('utf-8'))

        is_valid = logic.validate_intel(data)
        if not is_valid:
            producer_dlq.produce('intel_signals_dlq', json.dumps(data).encode('utf-8'))
            log_event('ERROR', f"invalid intel!!!")
            continue

        ent_id = data["entity_id"]
        previous_data = database.get_target(ent_id)
        distance = logic.calculate_dist(previous_data, {'lat': data['reported_lat'], 'lon': data['reported_lon']})

        database.upsert_target(ent_id, {'lat': data['reported_lat'], 'lon': data['reported_lon'],
                                        'priority': data.get('priority_level', 99),
                                        "last_signal": data["timestamp"]})
        log_event('INFO', f"intel updated for {ent_id} in DB")

if __name__ == "__main__":
    process_intel()
    


