import json
from confluent_kafka import Producer
from simulator import generate_attack_message, generate_damage_message, generate_intel_message

producer_config = {"bootstrap.servers": "localhost:9092"}
producer = Producer(producer_config)

def run_producer():
    while True:
        intel_msg = generate_intel_message()
        producer.produce('intel', intel_msg)

        attack_msg = generate_attack_message()
        producer.produce('attack', attack_msg)

        damage_msg = generate_damage_message()
        producer.produce('damage', damage_msg)

        def publish(event):
            value = json.dumps(event).encode("utf-8")
    

producer.flush()
   