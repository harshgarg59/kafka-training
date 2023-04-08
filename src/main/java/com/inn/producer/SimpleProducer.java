package com.inn.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class SimpleProducer {
    private static final Logger logger = LoggerFactory.getLogger(SimpleProducer.class);

    public static void main(String[] args) throws IOException {
        logger.info("creating kafka producer");
        Properties properties=new Properties();
        properties.load(new FileInputStream("src/main/resources/kafka.properties"));
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, SimpleProducer.class.getName());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,IntegerSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        logger.info("config {}",properties);

        KafkaProducer<Integer,String> producer=new KafkaProducer<Integer,String>(properties);
        logger.info("going to send messages");
        for (int i = 0; i < 100000; i++) {
            ProducerRecord<Integer,String> producerRecord=new ProducerRecord<>("Test",i,"Sample Message-"+i);
            producer.send(producerRecord);
        }
        logger.info("Finished sending messages");
        producer.close();
    }
}
