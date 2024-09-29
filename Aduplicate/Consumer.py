from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic

class Consumer(KafkaConsumer):
    def __init__(self, **configs):
        super().__init__(**configs)
        # Сохраняем аргументы в атрибутах экземпляра
        for key, value in configs.items():
            setattr(self, key, value)
        self._create_special_topic()

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