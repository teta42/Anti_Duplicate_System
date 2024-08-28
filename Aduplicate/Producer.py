import hashlib
import json
from kafka import KafkaProducer

class Producer(KafkaProducer):
    def __init__(self, **configs):
        super().__init__(**configs)
        
    def hesh(self, value: dict) -> dict:
        # Создание SHA-1 хеша
        hash_object = hashlib.sha1(json.dumps(value, sort_keys=True).encode())
        # Добавляем хеш в словарь
        value['hash'] = hash_object.hexdigest()
        # Возвращаем отсортированный словарь
        return dict(sorted(value.items()))
        
    def send(self, topic: str, value: dict):
        value = self.hesh(value)
        super().send(topic, value)
    
    