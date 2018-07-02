from kafka import KafkaProducer

class KafkaSender(object):

    def __init__(self, host='localhost', port=9092):
        self.producer = KafkaProducer(bootstrap_servers="{host}:{port}".format(host=host, port=port))

    def send_message(self, topic, message):
        self.producer.send(topic, message.encode('utf-8'))

    def close(self):
        self.producer.close()
