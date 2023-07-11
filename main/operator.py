from json import load, dump

if __name__ == "__main__":
    print("Topics...")
    print(f"Choices: 1) Create 2) Read 3) Delete 4) View all 5) Exit")

    with open("topics.json", "r") as infile:
        topic_msg = load(infile)

    try:
        while True:
            choice = input("Enter choice: ")

            if choice == "1":
                topic = input("Enter topic name: ")
                if topic in topic_msg.keys():
                    print("Topic already exists")
                else:
                    topic_msg[topic] = []

            elif choice == "2":
                topic = input("Enter topic name: ")
                if topic not in topic_msg.keys():
                    print("Topic does not exist")
                else:
                    print("Topic data:", topic_msg[topic])

            elif choice == "3":
                topic = input("Enter topic name: ")
                if topic not in topic_msg.keys():
                    print("Topic does not exist")
                else:
                    del topic_msg[topic]

            elif choice == "4":
                if topic_msg == {}:
                    print("No topics exist")
                else:
                    print("All topics:")
                    print(topic_msg)

            elif choice == "5":
                break

    except KeyboardInterrupt:
        with open("topics.json", "w") as outfile:
            dump(topic_msg, outfile, indent = 4)

        print("Broker 3 failed...")
        exit()

    with open("topics.json", "w") as outfile:
        dump(topic_msg, outfile, indent = 4)