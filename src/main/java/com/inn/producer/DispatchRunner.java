package com.inn.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class DispatchRunner {
    public final static String[] eventFiles = {"src/data/NSE05NOV2018BHAV.csv", "src/data/NSE06NOV2018BHAV.csv"};
    private static final Logger logger = LoggerFactory.getLogger(Dispatcher.class);

    public static void main(String[] args) throws IOException {
        Properties properties = new Properties();

        properties.load(new FileInputStream("src/main/resources/kafka.properties"));
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, SimpleProducer.class.getName());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<Integer, String> producer = new KafkaProducer<Integer, String>(properties);
        Thread[] threads = new Thread[eventFiles.length];
        logger.info("Dispatcher thread is starting");

        for (int i = 0; i < eventFiles.length; i++) {
            threads[i] = new Thread(new Dispatcher("nse-eod-topic", eventFiles[i], producer));
            threads[i].start();
        }

        try {
            for (Thread t : threads) t.join();
        } catch (Exception e) {
            logger.error("Error while joining threads" + e.getMessage());
        } finally {
            producer.close();
        }
    }
}
