import json
from myKafka import KafkaConsumer
import os.path
import csv
import sys

argument = None
if len(sys.argv) > 1:
    argument = sys.argv[1]

topicName = input("\nEnter topicName consumer subscribes to :")
consumer = KafkaConsumer(topicName, bootstrap_servers='localhost:9092')
topicLocation = str(str(consumer.broker_port) + "/" + topicName + ".csv")

oldData = []

if argument != '--from-beginning':
    if os.path.isfile(topicLocation):
        temp = []
        f = open(topicLocation, "r")
        reader = csv.reader(f)
        for row in reader:
            temp.append(row)
        f.close()

        oldData = temp

topicStatus = consumer.topic_status()
print("Connected to broker at port: " + str(consumer.broker_port))
while True:
    try:
        consumer.checkBroker()

        if os.path.isfile(topicLocation):
            temp = []
            f = open(topicLocation, "r")
            reader = csv.reader(f)
            for row in reader:
                temp.append(row)
            f.close()

            for newTopic in temp[len(oldData):]:
                print(newTopic)
                oldData = temp
        else:
            # print("Topic does not exist")
            pass

    except KeyboardInterrupt:
        consumer.close()
        consumer.reconnectToBroker()
        print('Unsubscribed!')
        break
