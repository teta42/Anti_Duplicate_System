from kafka import KafkaConsumer, TopicPartition, KafkaProducer
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
        
        self._GMST = _Get_message_for_special_topic(bootstrap_servers=self.get_config("bootstrap_servers"))
        # Создаём удобный словарь обработанных сообщений
        self._dict_of_verified_messages = self._GMST._local_db_of_checked_messages_hashes()

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
    
    def __next__(self):
        # Обновляем списки проверенных сообщений
        self._dict_of_verified_messages = self._GMST._local_db_of_checked_messages_hashes(self._dict_of_verified_messages)
        while True:
            # Получаем следующий элемент из родительского класса
            original_value = super().__next__()
            
            result = self._post_production(original_value)
            
            if result is not None:
                return result
    
    def _post_production(self, message: object) -> object:
        # Получаем список проверенных сообщений для конкретного раздела
        if message.partition in self._dict_of_verified_messages:
            verified_messages = self._dict_of_verified_messages[message.partition]
        else:
            verified_messages = []
        
        # Получаем хэш текущего сообщения
        message_hash = message.value['hash']
        
        # Проверяем, что хэш сообщения НЕ находится в списке проверенных сообщений
        if message_hash not in verified_messages:
            # Добавляем хэш в локальную базу данных проверенных сообщений
            if message.partition in self._dict_of_verified_messages:
                self._dict_of_verified_messages[message.partition].append(message_hash)
            else:
                self._dict_of_verified_messages[message.partition] = [message_hash]
                
            # Инициализируем продюсера Kafka
            kafka_producer = KafkaProducer(
                bootstrap_servers=self.get_config("bootstrap_servers"),  # Адрес вашего Kafka брокера
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),  # Сериализация сообщений в JSON
                acks=1,
                retries=10
            )  

            # Создаем сообщение для отправки в специальную тему
            message_to_send = {
                "hash": message_hash,
                "topic": message.topic,
                "partition": message.partition
            }
            
            # Отправляем сообщение в 'special_topic'
            kafka_producer.send('special_topic', message_to_send)

            # Ждем, пока все сообщения будут отправлены
            kafka_producer.flush()

            # Закрываем продюсера
            kafka_producer.close()
            
            # Удаляем системную информацию
            del message.value['hash']
            
            return message


    
    
class _Get_message_for_special_topic():
    '''Класс нужен для работы с проверенными 
    сообщениями из специальной темы'''
    
    def __init__(self, bootstrap_servers: str) -> None:
        self.bootstrap_servers = bootstrap_servers
        self._list_offset = []
        
        self._all_special_messages = self.all_messages()
        self._max_offset = self.max_offset()
        
    def create_consumer(self, bootstrap_servers: str) -> object:
        # Создаем потребителя
        consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        return consumer
        
    def all_messages(self) -> list:
        '''Здесь мы получаем список всех 
        проверенных/обработанных сообщений из всех тем и партиций
        а также максиальный offset'''
        consumer = self.create_consumer(self.bootstrap_servers)
        
        tp = TopicPartition("special_topic", 0)
        consumer.assign([tp])
        poll = consumer.poll(timeout_ms=1000)
        
        messages = []
        
        # Получение сообщений
        try:
            for tp, messages_list in poll.items():  # Извлекаем сообщения из словаря
                for message in messages_list:  # Перебираем каждое сообщение
                    messages.append(message.value)
                    self._list_offset.append(message.offset)
        except KeyboardInterrupt:
            print("Остановка (внутреннего) потребителя.")
        finally:
            consumer.close()
        
        return messages

    def new_messages(self) -> list:
        consumer = self.create_consumer(self.bootstrap_servers)

        # Подписка на топик и партицию
        tp = TopicPartition("special_topic", 0)
        consumer.assign([tp])

        # Установка позиции на указанный offset
        consumer.seek(tp, self._max_offset)

        poll = consumer.poll(timeout_ms=1000)
        messages = []
        try:
            for tp, messages_list in poll.items():  # Извлекаем сообщения из словаря
                for message in messages_list:  # Перебираем каждое сообщение
                    messages.append(message.value)
                    self._list_offset.append(message.offset)
        except KeyboardInterrupt:
            print("Остановка (внутренего) потребителя.")
        finally:
            consumer.close()
        
        if self._list_offset:
            self._max_offset = self.max_offset()
        
        return messages

    def max_offset(self) -> int:
        if self._list_offset != []:
            return max(self._list_offset) + 1
        else:
            return 0

    def _local_db_of_checked_messages_hashes(self, partitioned_data={}) -> dict:
        '''Данная функция создаёт удобный словарь с хэшами проверенных сообщений'''

        if partitioned_data != {}:
            data = self.new_messages()
        else:
            data = self._all_special_messages

        # Проверка на наличие сообщений
        if data != []:
            # Разделяем данные по partition
            for item in data:
                partition = item["partition"]
                if partition not in partitioned_data:
                    partitioned_data[partition] = []
                partitioned_data[partition].append(item["hash"])

        return partitioned_data

    