package com.inn.single;

import com.inn.config.AppConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class HelloProducer {
    private static final Logger logger= LoggerFactory.getLogger(HelloProducer.class);

    public static void main(String[] args) {
        logger.info("Producer is created");
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.CLIENT_ID_CONFIG, HelloProducer.class.getName());
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfig.kafkabroker);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<Integer, String> producer = new KafkaProducer<>(properties);
        logger.info("start sending messages");

        for (int i = 0; i < AppConfig.numEvents; i++) {
            ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>(AppConfig.topicname, i, "SimpleMessage" + i);
            producer.send(producerRecord);
        }
        producer.close();
        logger.info("Producer is closed");



    }
}
