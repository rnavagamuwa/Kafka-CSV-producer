package com.rnavagamuwa;


import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * @author rnavagamuwa
 */
public class KafkaCsvProducer {
    private static final Logger logger = LogManager.getLogger(KafkaCsvProducer.class);
    private static String kafkaTopic = null;
    private static String kafkaJavaConfigLocation;

    private CSVReader csvReader;

    private Producer<String, String> ProducerProperties() {
        final Properties properties = loadConfig(kafkaJavaConfigLocation);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        logger.info("Successfully loaded Kafka properties.");
        return new KafkaProducer<>(properties);
    }

    public KafkaCsvProducer(String kafkaTopic,CSVReader csvReader,String kafkaJavaConfigLocation) {
        KafkaCsvProducer.kafkaTopic = kafkaTopic;
        this.csvReader = csvReader;
        KafkaCsvProducer.kafkaJavaConfigLocation = kafkaJavaConfigLocation;
    }

    public void PublishMessages() {
        logger.info("Started publishing messages ...");
        final Producer<String, String> CsvProducer = ProducerProperties();
        try (BufferedReader br = this.csvReader.readCSV()) {
            String line;
            boolean isTitleRow = true;
            long publishedCount = 0;
            long totalRowCount = 0;
            String[] columnNames = new String[0];
            logger.info("Started publishing events to the kafka topic : {} ...",kafkaTopic);
            while ((line = br.readLine()) != null) {

                if (isTitleRow) {
                    isTitleRow = false;
                    columnNames = line.split(",");
                    continue;
                }
                totalRowCount++;
                String[] values = line.split(",");
                Map<String, Object> map = new HashMap<>();

                if (values.length != columnNames.length) {
                    continue;
                }

                for (int i = 0; i < values.length; i++) {
                    String value = removeDoubleQuotesIfThereAre(values[i]);
                    if (isInt(value)) {
                        map.put(columnNames[i], Integer.parseInt(value));
                    } else if (isDouble(value)) {
                        map.put(columnNames[i], Double.parseDouble(value));
                    } else {
                        map.put(columnNames[i], value);
                    }

                }

                String record = new ObjectMapper().writeValueAsString(map);
                CsvProducer.send(new ProducerRecord<>(kafkaTopic, UUID.randomUUID().toString(), record), (m, e) -> {
                    if (e != null) {
                        logger.error("Failed to publish record : {}", record, e);
                    }
                });
                publishedCount++;
                if (publishedCount / 100000 > 1) {
                    logger.info("{} events has been published.", publishedCount);
                }
            }
            logger.info("Completed publishing messages.");
            logger.info("Total row count : {} | Published row count : {}", totalRowCount, publishedCount);
        } catch (IOException e) {
            logger.error("Error when reading the file", e);
        }
    }

    private Properties loadConfig(final String configFile) {
        if (!Files.exists(Paths.get(configFile))) {
            logger.error("Failed to find the config file from given path : {}", configFile);
            throw new RuntimeException("Failed to find the config file from given path : " + configFile);
        }
        final Properties cfg = new Properties();
        try (InputStream inputStream = new FileInputStream(configFile)) {
            cfg.load(inputStream);
            return cfg;
        } catch (IOException ex) {
            logger.error("Failed to find the config file from given path : {}", configFile);
            throw new RuntimeException("Failed to find the config file from given path : " + configFile);
        }
    }

    private String removeDoubleQuotesIfThereAre(String input) {
        if (input.startsWith("\"") && input.endsWith("\"")) {
            return input.substring(1, input.length() - 1);
        } else {
            return input;
        }
    }

    private boolean isInt(String input) {
        try {
            Integer.parseInt(input);
            return true;
        } catch (NumberFormatException ex) {
            return false;
        }
    }

    private boolean isDouble(String input) {
        try {
            Double.parseDouble(input);
            return true;
        } catch (NumberFormatException ex) {
            return false;
        }
    }
}