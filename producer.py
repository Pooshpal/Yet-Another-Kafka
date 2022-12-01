from myKafka import KafkaProducer

print('Kafka Producer is initializing...')
bootstrap_servers = ['localhost:9092']
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
print('Kafka Producer has been initiated...')

while True:
    data = input()
    try:
        ack = producer.send(data)
    except BrokenPipeError:
        print('Broker is down. Reconnecting...')
        producer.reconnectToBroker()
    except Exception as e:
        print(e)
        if e == "[Errno 104] Connection reset by peer":
            producer.reconnectToBroker()

