from kafka import KafkaConsumer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic
import json

class Consumer(KafkaConsumer):
    def __init__(self, **configs):
        super().__init__(**configs)
        # Сохраняем аргументы в атрибутах экземпляра
        for key, value in configs.items():
            setattr(self, key, value)
        self._create_special_topic()
        self._special_message = self._get_messages_special_topic()

    def get_config(self, key):
        """Метод для получения значения конфигурации по ключу."""
        return getattr(self, key, None)

    def _create_special_topic(self) -> None:
        # Создаем экземпляр KafkaAdminClient
        admin_client = KafkaAdminClient(bootstrap_servers=self.get_config("bootstrap_servers"))

        # Проверяем существование топика
        existing_topics = admin_client.list_topics()
        if "special_topic" not in existing_topics:
            # Определяем новую тему
            new_topic = NewTopic(name="special_topic", num_partitions=1, replication_factor=1)

            # Создаем тему
            admin_client.create_topics(new_topics=[new_topic], validate_only=False)
    
    def _get_messages_special_topic(self) -> list:
        # Создаем потребителя
        consumer = KafkaConsumer(
            bootstrap_servers=self.get_config("bootstrap_servers"),
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

        # Указываем партицию
        partition = TopicPartition('special_topic', 0)  # 0 - номер партиции
        consumer.assign([partition])

        messages = []

        # Проверяем наличие сообщений
        poll_result = consumer.poll(timeout_ms=1000)  # Устанавливаем таймаут для ожидания сообщений

        if not poll_result:  # Если нет сообщений
            return messages  # Возвращаем пустой список

        # Читаем сообщения с установленного offset
        for _, message_list in poll_result.items():
            for message in message_list:
                messages.append(message.value)

        return messages

