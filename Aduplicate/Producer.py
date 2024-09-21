import hashlib
import json
import random
from kafka import KafkaProducer

class Producer(KafkaProducer):
    def __init__(self, **configs):
        # Инициализация родительского класса
        super().__init__(**configs)
        
    # Получение хеша и его добовление
    def hesh(self, value: dict) -> dict:
        # Создание SHA-1 хеша
        hash_object = hashlib.sha1(json.dumps(value, sort_keys=True).encode())
        # Добавляем хеш в словарь
        value['hash'] = hash_object.hexdigest()
        # Возвращаем отсортированный словарь
        return dict(sorted(value.items()))
        
    # Получение случайной партиции из темы
    def _parts(self, topic: str) -> list:
        return random.choice(list(self.partitions_for(topic)))
        
    # отправка сообщения
    def send(self, topic: str, value: dict):
        value = self.hesh(value)
        super().send(topic, value, partition=self._parts(topic))