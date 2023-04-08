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

public class TransectionalProducer {
    private static final Logger logger = LoggerFactory.getLogger(TransectionalProducer.class);

    public static void main(String[] args) throws IOException {
        logger.info("creating kafka producer");
        Properties properties=new Properties();
        properties.load(new FileInputStream("src/main/resources/kafka.properties"));
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, SimpleProducer.class.getName());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,TransectionalProducer.class.getName());
        logger.info("config {}",properties);

        KafkaProducer<Integer,String> producer=new KafkaProducer<>(properties);
        producer.initTransactions();
        producer.beginTransaction();
        logger.info("going to send messages");
        try {
            for (int i = 0; i < 100000; i++) {
                ProducerRecord<Integer,String> record1=new ProducerRecord<>("Test-1",i,"Sample Message-T1-"+i);
                ProducerRecord<Integer,String> record2=new ProducerRecord<>("Test-2",i,"Sample Message-T1-"+i);
                producer.send(record1);
                producer.send(record2);
            }
        } catch (Exception e) {
            producer.abortTransaction();
            throw new RuntimeException(e);
        } finally {
            producer.close();
        }
        producer.commitTransaction();
        logger.info("Finished sending messages");
        producer.close();
    }
}
