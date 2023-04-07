package com.inn.mullti;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Scanner;

public class Dispatcher implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(Dispatcher.class);
    private String filename;
    private String topicName;
    private KafkaProducer<Integer, String> producer;

    public Dispatcher(String filename, String topicName, KafkaProducer<Integer, String> producer) {
        this.filename = filename;
        this.topicName = topicName;
        this.producer = producer;
    }

    @Override
    public void run() {
        logger.info("Producer is started");
        File file = new File(filename);
        int counter = 0;
        try (Scanner scanner = new Scanner(file)) {
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                producer.send(new ProducerRecord<Integer, String>(topicName, null, line));
                counter++;
            }
            logger.info("counter is {} from file {}",counter,filename);
            logger.info("Finished for {}",filename);
        } catch (Exception e) {
            logger.error("Exception while sending msg to kafka {}",e.getStackTrace());
        }

    }
}
