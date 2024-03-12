from confluent_kafka import Producer

# Конфигурация Kafka
conf = {'bootstrap.servers': 'localhost:9092'}

# Создание объекта Producer
producer = Producer(conf)


# Отправка сообщения
def delivery_report(err, msg):
    if err is not None:
        print('Сообщение не было доставлено: {}'.format(err))
    else:
        print('Сообщение успешно доставлено: {}'.format(msg.value().decode('utf-8')))


# Отправка сообщения в тему "Simple"
producer.produce('Simple', key='Simple', value='Привет, Kafka!', callback=delivery_report)

# Ждем, пока все сообщения будут отправлены
producer.flush()
