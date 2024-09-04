# Kafka Protobuf Data Processing Project

This project demonstrates a Kafka-based data processing system using Protocol Buffers (protobuf) for message serialization. It includes a producer that generates random numbers and multiple consumers that process these numbers in different ways, utilizing Schema Registry for schema evolution.

## Project Structure

- `docker-compose.yml`: Defines the Kafka ecosystem services, including Zookeeper, Kafka brokers, Schema Registry, and additional tools.
- `my_event.proto`: Protocol Buffers definition file.
- `my_event_pb2.py`: Generated Python code from the .proto file.
- `create_topic.ipynb`: Jupyter notebook for creating the Kafka topic.
- `producer.ipynb`: Jupyter notebook for the Kafka producer.
- `consumer_prime.ipynb`: Jupyter notebook for the prime-checking consumer.
- `consumer_sqrt.ipynb`: Jupyter notebook for the square root consumer.
- `consumer_square.ipynb`: Jupyter notebook for the square calculator consumer.

## Prerequisites

- Docker and Docker Compose
- Python 3.x
- Jupyter Notebook

## Setup

1. Clone this repository:
   ```
   git clone <repository-url>
   cd <repository-directory>
   ```

2. Start the Kafka ecosystem:
   ```
   docker-compose up -d
   ```

## Running the Project

1. Start Jupyter Notebook:
   ```
   jupyter notebook
   ```

2. Open and run the `create_topic.ipynb` notebook to create the Kafka topic.

3. Open and run the `producer.ipynb` notebook to start generating random numbers.

4. In separate notebook instances, open and run `consumer_prime.ipynb`, `consumer_sqrt.ipynb`, and `consumer_square.ipynb` to start processing the data.

## What it does

- The create topic notebook sets up the Kafka topic with the desired configuration.
- The producer generates random integers between 1 and 100 every 5 seconds and sends them to a Kafka topic.
- The prime consumer checks if each number is prime.
- The square root consumer calculates the square root of each number.
- The square calculator consumer computes the square of each number.
- All messages are serialized using Protocol Buffers and the schema is registered with Schema Registry.

## Consumer Groups

The consumers in this project use consumer groups to allow for scalable and fault-tolerant processing:

- All consumers are part of the same consumer group (`mygroup`).
- This setup allows for load balancing: if you run multiple instances of a consumer, Kafka will distribute the partitions among them.
- If a consumer fails, its partitions will be reassigned to other consumers in the group.

To demonstrate this:
1. Run multiple instances of a consumer notebook.
2. Observe how the processing is distributed among the instances.
3. Stop one instance and see how Kafka reassigns its work to the remaining instances.

## Schema Registry

This project uses Confluent's Schema Registry to manage the Protobuf schemas:

- The Schema Registry is running as a Docker container defined in the `docker-compose.yml` file.
- The producer registers the schema when sending messages.
- Consumers use the Schema Registry to deserialize the messages.
- This setup allows for schema evolution while maintaining backward/forward compatibility.

You can view registered schemas by accessing the Schema Registry REST API:
```
curl http://localhost:8081/subjects
```

## Extending the Project

You can add more consumers or modify the existing ones to perform different calculations or data processing tasks. When modifying the schema:

1. Update the `my_event.proto` file.
2. Regenerate the `my_event_pb2.py` file.
3. Update the producer and consumers to use the new schema.
4. The Schema Registry will handle versioning of your schema.

## Troubleshooting

- Ensure all Docker containers are running: `docker-compose ps`
- Check Kafka logs: `docker-compose logs kafka-1`
- Verify topic creation: `docker-compose exec kafka-1 kafka-topics --list --bootstrap-server kafka-1:29092`
- Check Schema Registry: `curl http://localhost:8081/subjects`

## License

This project is open source and available under the [MIT License](LICENSE).