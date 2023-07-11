import socket
from sys import argv

if __name__ == "__main__":
    brkr = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        brkr.connect(("127.0.0.1", 9093))
    except:
        try:
            brkr.connect(("127.0.0.1", 9094))
        except:
            try:
                brkr.connect(("127.0.0.1", 9095))
            except:
                print("No brokers active...")
                exit()
    print("Consumer...")

    if len(argv) == 2:
        type = "cons"
    elif len(argv) == 3:
        type = "cons-fb"
    topic = argv[1]

    brkr.send(bytes(type, "utf-8"))
    print("Sent:", type)
    ack = brkr.recv(1024).decode()
    print("Rcvd:", ack)

    brkr.send(bytes(topic, "utf-8"))
    print("Sent:", topic)
    ack = brkr.recv(1024).decode()
    print("Rcvd:", ack)

    if type == "cons-fb":
        while True:
            msg = brkr.recv(1024).decode()
            if not msg:
                break
            print("Rcvd:", msg)
            brkr.send(bytes(ack, "utf-8"))
            print("Sent:", ack)

    while True:
        try:
            msg = brkr.recv(1024).decode()
            if msg:
                print("Rcvd:", msg)   
            try:
                brkr.send(bytes("ACK", "utf-8"))
                print("Sent: ACK")
            except:
                brkr.close()
                brkr = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
                try:         
                    brkr.connect(("127.0.0.1", 9094))
                except:
                    try:
                        brkr.connect(("127.0.0.1", 9095))
                    except:
                        print("All brokers failed...")
                        brkr.close()
                        exit()

                print("Consumer...")

                if len(argv) == 2:
                    type = "cons"
                elif len(argv) == 3:
                    type = "cons-fb"
                topic = argv[1]

                brkr.send(bytes(type, "utf-8"))
                print("Sent:", type)
                ack = brkr.recv(1024).decode()
                print("Rcvd:", ack)

                brkr.send(bytes(topic, "utf-8"))
                print("Sent:", topic)
                ack = brkr.recv(1024).decode()
                print("Rcvd:", ack)
            
        except KeyboardInterrupt:
            print("Consumer failed...")
            exit()