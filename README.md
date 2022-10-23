# Kafka CSV Producer

This can be used to push records from a CSV to a Kafka topic. The records will be passed as JSON objects and the 1st row of the CSV file will be used to generate the field names.

### Prerequisites 

- Java 17
- Apache Maven 3.8.5

### How to build the tool

- Execute `mvn clean install` in the root directory.
- Once the above command has been executed successfully, the distribution can be found in the `/target/kafka-csv-producer-1.0-SNAPSHOT-distribution.zip`
- Extract the `kafka-csv-producer-1.0-SNAPSHOT-distribution.zip` distribution.

### How to run the tool

- Update the `kafka.config` file with the correct confluent cloud details, or else a different `kafka.config` file can be passed as an argument.
- Execute the below command

        java -cp kafka-csv-producer-1.0-SNAPSHOT.jar com.rnavagamuwa.CLI --csv-path <path_to_the_local_csv_file> --kafka-topic <kafka_topic_name> --kafka-config <path_to_the_kafka_config_file>

