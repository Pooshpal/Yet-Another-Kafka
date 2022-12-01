import socket
import sys
import threading
import csv
import os.path
import os
from time import sleep


# Kafka Broker
class Broker():
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.conn = None

        self.producers = []
        self.consumers = {}

        # Host and port of the Zookeeper
        HOST, PORT = "localhost", 9092

        # Connect to Zookeeper
        sock_zookeeper = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock_zookeeper.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock_zookeeper.connect((HOST, PORT))
        self.conn = sock_zookeeper
        print('Broker has connected to Zookeeper')
        self.conn.send(f"Broker {self.port}".encode('utf-8'))

        # Start heartbeat thread
        threading.Thread(target=self.zookeeper_heartbeat, args=(self.conn,)).start()

        # Listen for connections
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((self.host, self.port))
        sock.listen(1)
        print('Broker is listening on port ' + str(self.port) + '...')

        while True:
            self.conn, self.addr = sock.accept()
            print('Broker has accepted a connection from ' + str(self.addr))

            prodcons = self.conn.recv(2048).decode('utf-8')
            self.conn.sendall("ack".encode('utf-8'))

            if prodcons == 'Producer':
                self.producers.append(self.addr[1])
                threading.Thread(target=self.multi_threaded_publisher, args=(self.conn, self.addr)).start()
            elif prodcons == 'Consumer':
                topic = self.conn.recv(2048).decode('utf-8')
                try:
                    if topic in self.consumers:
                        self.consumers[topic].append(self.conn)
                    else:
                        self.consumers[topic] = [self.conn]
                except Exception as e:
                    self.consumers[topic].remove(self.conn)
                    self.consumers[topic].append(self.conn)

    def multi_threaded_publisher(self, conn, addr):
        while True:
            data = conn.recv(2048)
            conn.sendall("ack".encode('utf-8'))

            if not data:
                continue

            else:
                topic, value = data.decode('utf-8').split(':', 1)
                print('Broker has received a message from ' + str(addr) + ': ' + topic + ' ' + value)

                if value:
                    # store this new data into directory of file
                    topicLocation1 = str("9095" + '/' + topic + '.csv')
                    topicLocation2 = str("9096" + '/' + topic + '.csv')
                    topicLocation3 = str("9097" + '/' + topic + '.csv')

                    for topicLocation in [topicLocation1, topicLocation2, topicLocation3]:
                        # topic already exists
                        if os.path.isfile(topicLocation) == True:

                            f = open(topicLocation, "a")
                            writer = csv.writer(f)
                            writer.writerow([value])
                            f.close()
                        # topic is new
                        else:
                            os.system('touch ' + topicLocation)
                            f = open(topicLocation, "w")
                            writer = csv.writer(f)
                            writer.writerow([value])
                            f.close()

    def zookeeper_heartbeat(self, conn):
        conn.send('Broker'.encode('utf-8'))
        while True:
            conn.send(f'Broker {self.host}:{self.port} is alive'.encode('utf-8'))
            ack = conn.recv(2048).decode('utf-8')
            sleep(0.5)


host, port = sys.argv[1].split(':')
Broker = Broker(host, int(port))  # type: ignore
