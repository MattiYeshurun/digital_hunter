import json
from confluent_kafka import Consumer, Producer
import database
from logger import log_event


consumer_config = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "attack_group",
    "auto.offset.reset": "earliest"
}

consumer = Consumer(consumer_config)
consumer.subscribe(['attack'])

producer_dlq = Producer({"bootstrap.servers": "localhost:9092"})

def process_attack():
    print("Attack Service is active")
    while True:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue
        data = json.loads(msg.value().decode('utf-8'))

        ent_id = data["entity_id"]
        
        database.upsert_target(ent_id, {"dtatus": "attacked", "last_attack": data['timestamp']})
        log_event('INFO', f"Target {ent_id} is attacked", {"weapon": data['weapon_type']})

if __name__ == "__main__":
    process_attack()
    


