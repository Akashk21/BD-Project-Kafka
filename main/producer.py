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
    print("Producer...")

    type = "prod"
    print("Sent:", type)
    brkr.send(bytes(type, "utf-8"))
    ack = brkr.recv(1024).decode()
    print("Rcvd:", ack)

    topic = argv[1]
    print("Sent:", topic)
    brkr.send(bytes(topic,"utf-8"))
    ack = brkr.recv(1024).decode()
    print("Rcvd:", ack)
        
    while True:
        try:
            data = input("Sent: ")

            if data == "":
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
                
                print("Producer...")

                type = "prod"
                print("Sent:", type)
                brkr.send(bytes(type, "utf-8"))
                ack = brkr.recv(1024).decode()
                print("Rcvd:", ack)

                topic = argv[1]
                print("Sent:", topic)
                brkr.send(bytes(topic,"utf-8"))
                ack = brkr.recv(1024).decode()
                print("Rcvd:", ack)

            brkr.send(bytes(data,"utf-8"))
            if data != "":
                ack = brkr.recv(1024).decode()
                print("Rcvd:", ack)
        
        except KeyboardInterrupt:
            print("Producer failed...")
            exit()