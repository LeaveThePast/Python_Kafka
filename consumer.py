from confluent_kafka import Consumer, KafkaError

# Конфигурация Kafka
conf = {'bootstrap.servers': 'localhost:9092', 'group.id': 'my_consumer_group', 'auto.offset.reset': 'earliest'}

# Создание объекта Consumer
consumer = Consumer(conf)

# Подписка на тему "Simple"
consumer.subscribe(['Simple'])

# Чтение и обработка сообщений
while True:
    msg = consumer.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            continue
        else:
            print(msg.error())
            break

    print('Получено сообщение: {}'.format(msg.value().decode('utf-8')))