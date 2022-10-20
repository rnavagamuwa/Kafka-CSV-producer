package com.rnavagamuwa;

import java.io.File;
import java.nio.file.Paths;

/**
 * @author rnavagamuwa
 */
@CommandLine.Command(
        name = "hello",
        description = "Says hello"
)
public class CLI implements Runnable {

    @CommandLine.Option(names = {"--kafka-config"})
    private String kafkaConfig = Paths.get("").toAbsolutePath() + File.separator + "kafka.config";

    @CommandLine.Option(required = true, names = {"--csv-path"})
    private String csvLocation;

    @CommandLine.Option(required = true, names = {"--kafka-topic"})
    private String kafkaTopic;

    public static void main(String[] args) {
        CommandLine.run(new CLI(), args);
    }

    @Override
    public void run() {
        KafkaCsvProducer kafkaCsvProducer = new KafkaCsvProducer(kafkaTopic, csvLocation, kafkaConfig);
        kafkaCsvProducer.PublishMessages();
    }
}