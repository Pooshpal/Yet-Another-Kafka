import socket
import threading
import time


class Zookeeper:
    def __init__(self):
        self.HOST = "localhost"
        self.PORT = 9092
        self.conn = None

        host = "localhost"

        self.alive_brokers = []

        # Listen for broker connections
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((self.HOST, self.PORT))
        sock.listen(1)

        while True:
            print(self.alive_brokers)
            self.conn, self.addr = sock.accept()
            print('Zookeeper has accepted a connection from ' + str(self.addr))
            data = self.conn.recv(2048)
            if data.decode('utf-8').split(' ')[0] != 'Broker':
                port = self.alive_brokers[0]
                self.conn.send(str(port).encode('utf-8'))
                self.conn.close()
            else:
                self.alive_brokers.append(data.decode('utf-8').split(' ')[1])
                threading.Thread(target=self.multi_threaded_broker, args=[self.conn, self.addr]).start()

    def multi_threaded_broker(self, conn, addr):
        while True:

            try:
                data = conn.recv(2048)
                time.sleep(1)
                print('mini Zookeeper has received a message from ' + str(addr) + ': ' + data.decode('utf-8'))
                conn.sendall("ack".encode('utf-8'))
            except Exception as e:
                print(e)
                self.alive_brokers = self.alive_brokers[1:]
                break

        conn.close()


Zookeeper()
