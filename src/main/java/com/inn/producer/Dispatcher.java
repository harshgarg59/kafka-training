package com.inn.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

public class Dispatcher implements Runnable {
    Logger logger = LoggerFactory.getLogger(Dispatcher.class);

    String topicname;
    String filepath;
    KafkaProducer<Integer, String> producer;

    public Dispatcher(String topicname, String filepath, KafkaProducer<Integer, String> producer) {
        this.topicname = topicname;
        this.filepath = filepath;
        this.producer = producer;
    }

    @Override
    public void run() {
        logger.info("start producer " + filepath);
        File file = new File(filepath);
        int count = 0;
        try {
            Scanner scanner = new Scanner(file);
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>(topicname, null, line);
                producer.send(producerRecord);
                count++;
            }
            logger.info("Message sent" + count + "from file " + filepath);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }


    }
}
