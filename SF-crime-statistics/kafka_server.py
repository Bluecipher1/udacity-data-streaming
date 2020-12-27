import producer_server


def run_kafka_server():
	# get the json file path
    input_file = "./police-department-calls-for-service.json"

    producer = producer_server.ProducerServer(
        input_file=input_file,
        topic="com.udacity.crimestatistics",
        bootstrap_servers="localhost:9092",
        client_id="crimestatistics"
    )

    return producer


def feed():
    producer = run_kafka_server()
    producer.generate_data()


if __name__ == "__main__":
    feed()
