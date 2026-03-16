import json
from confluent_kafka import Consumer, Producer
import database
from logger import log_event


consumer_config = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "damage_group",
    "auto.offset.reset": "earliest"
}

consumer = Consumer(consumer_config)
consumer.subscribe(['damage'])

producer_dlq = Producer({"bootstrap.servers": "localhost:9092"})

def process_damage():
    print("Damage Service is active")
    while True:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue
        data = json.loads(msg.value().decode('utf-8'))

        ent_id = data.get('entity_id')
        result = data.get('rasult')
        
        database.upsert_target(ent_id, {"dtatus": result, "damage_timestamp": data['timestamp']})
        log_event('INFO', f"Damage report for {ent_id}: {result}")

if __name__ == "__main__":
    process_damage()
    


