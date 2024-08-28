from kafka import KafkaProducer
from kafka.serializer import Serializer
import hashlib
import json

class Producer():
    def __init__(self, bootstrap_servers='localhost:9092', 
                 key_serializer=Serializer(), 
                 value_serializer=Serializer(),
                 acks=1,
                 linger_ms=100,
                 retries=5) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.key_serializer = key_serializer
        self.value_serializer = value_serializer
        self.acks = acks
        self.linger_ms = linger_ms
        self.retries = retries
        
        self.kafka_producer = KafkaProducer(bootstrap_servers=self.bootstrap_servers,
                                            key_serializer=self.key_serializer, 
                                            value_serializer=self.value_serializer,
                                            acks=self.acks,
                                            linger_ms=self.linger_ms,
                                            retries=self.retries)
    
    def send(self, topic: str, value: dict):
        # Создание SHA-1 хеша
        hash_object = hashlib.sha1(json.dumps(value, sort_keys=True).encode())
        value['hash'] = hash_object.hexdigest()  # Добавляем хеш в словарь
        # Преобразование обратно в строку JSON
        value = json.dumps(value, sort_keys=True)
        
        self.kafka_producer.send(topic, value)
        