package com.rnavagamuwa;

/**
 * @author rnavagamuwa
 */
public class Main {
    public static void main(String[] args) {
        KafkaCsvProducer kafkaCsvProducer = new KafkaCsvProducer();
        kafkaCsvProducer.initialize(args);
        kafkaCsvProducer.PublishMessages();
    }
}
