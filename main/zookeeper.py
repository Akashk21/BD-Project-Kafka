import socket

if __name__ == "__main__":
    print("Zookeeper...")

    try:
        brkr1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)         
        brkr1.bind(("127.0.0.1", 9090))

        brkr2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)         
        brkr2.bind(("127.0.0.1", 9091))

        brkr3 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)         
        brkr3.bind(("127.0.0.1", 9092))

        brkr1.listen(5)
        conn1, addr1 = brkr1.accept()

        brkr2.listen(5)
        conn2, addr2 = brkr2.accept()

        brkr3.listen(5)
        conn3, addr3 = brkr3.accept()   

        while True:
            conn1.send(bytes("1", "utf-8"))
            conn2.send(bytes("1", "utf-8"))
            conn3.send(bytes("1", "utf-8"))

            print("Sent: poll")

            beat1 = conn1.recv(1024).decode()
            beat2 = conn2.recv(1024).decode()
            beat3 = conn3.recv(1024).decode()

            print("Rcvd:", beat1)
            print("Rcvd:", beat2)
            print("Rcvd:", beat3)

            if beat1 != "BEAT":
                print("Broker 1 failed...")
                print("Switching to another broker...")
                break

        while True:
            conn2.send(bytes("2", "utf-8"))
            conn3.send(bytes("2", "utf-8"))

            print("Sent: poll")

            beat2 = conn2.recv(1024).decode()
            beat3 = conn3.recv(1024).decode()

            print("Rcvd:", beat2)
            print("Rcvd:", beat3)

            if beat2 != "BEAT":
                print("Broker 2 failed...")
                print("Switching to another broker...")
                break

        while True:
            conn3.send(bytes("3", "utf-8"))

            print("Sent: poll")

            beat3 = conn3.recv(1024).decode()

            print("Rcvd:", beat3)

            if beat3 != "BEAT":
                print("Broker 3 failed...")
                print("All brokers failed...")
                exit()

    except KeyboardInterrupt:
        print("Zookeeper failed...")
        exit()