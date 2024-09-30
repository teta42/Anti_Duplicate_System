from kafka import KafkaConsumer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic
import json

class Consumer(KafkaConsumer):
    '''
    Данный класс это надстройка над классом KafkaConsumer
    Нужен этот класс для гарантии того что одно сообщение обработается ровно 1 раз
    '''
    def __init__(self, **configs):
        super().__init__(**configs)
        # Сохраняем аргументы в атрибутах экземпляра
        for key, value in configs.items():
            setattr(self, key, value)
        self._create_special_topic() # Проверяем создание и создаём спец. тему
        
        # Создаём удобный словарь обработанных сообщений
        self._dict_of_verified_messages = self._local_db_of_checked_messages_hashes()

    def get_config(self, key):
        '''Метод для получения значения конфигурации по ключу'''
        return getattr(self, key, None)

    def _create_special_topic(self) -> None:
        '''Здесь проверяется создание специальной темы, для хранения всех
        обработанных сообщений'''
        
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
        '''Здесь мы получаем список всех 
        проверенных/обработанных сообщений из всех тем и партиций'''
        
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

    def _local_db_of_checked_messages_hashes(self) -> dict:
        '''Данная функция создаёт удобный словарь с хэшами проверенных сообщений'''
        
        # Получение списка проверенных сообщений
        special_messages = self._get_messages_special_topic()
        
        # Проверка на наличие сообщений
        if special_messages != []:
            
            # Создаем словарь для хранения списков по partition
            partitioned_data = {}

            # Разделяем данные по partition
            for item in special_messages:
                partition = item["partition"]
                if partition not in partitioned_data:
                    partitioned_data[partition] = []
                partitioned_data[partition].append(item["hash"])

            return partitioned_data
    
    def __next__(self):
        # Получаем следующий элемент из родительского класса
        original_value = super().__next__()
        self._post_production(original_value)
        
        return original_value
    
    def _post_production(self, obj: object) -> object:
        pass
    