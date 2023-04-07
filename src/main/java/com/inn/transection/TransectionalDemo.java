package com.inn.transection;

import com.inn.config.AppConfig;
import com.inn.config.TransectionalAppConfig;
import com.inn.single.HelloProducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;


public class TransectionalDemo {
    private static final Logger logger = LoggerFactory.getLogger(TransectionalDemo.class);

    public static void main(String[] args) throws Exception {
        logger.info("Producer is created");
        Properties properties = new Properties();
        FileInputStream fileInputStream = new FileInputStream(new File("src/main/resources/kafka.properties"));
        properties.load(fileInputStream);
        properties.setProperty(ProducerConfig.CLIENT_ID_CONFIG, TransectionalAppConfig.applicationID);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, TransectionalAppConfig.transaction_id);
        KafkaProducer<Integer, String> producer = new KafkaProducer<>(properties);
        logger.info("start initialization");
        producer.initTransactions();
        logger.info("initialization Complted... sending messages");
        producer.beginTransaction();
        try {
            for (int i = 0; i < TransectionalAppConfig.numEvents; i++) {
                ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>(TransectionalAppConfig.topicName1, i, "SimpleMessage" + i);
                ProducerRecord<Integer, String> producerRecord2 = new ProducerRecord<>(TransectionalAppConfig.topicName2, i, "SimpleMessage" + i);
                producer.send(producerRecord);
                producer.send(producerRecord2);
            }
        } catch (Exception e) {
           logger.error("Exepction while saving records to producer {}",e.getMessage());
            producer.abortTransaction();
        }
        producer.commitTransaction();
        producer.close();
        logger.info("Producer is closed");


    }
}
