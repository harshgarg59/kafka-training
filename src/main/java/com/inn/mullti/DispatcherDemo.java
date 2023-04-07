package com.inn.mullti;

import com.inn.config.MultiThreadAppConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

public class DispatcherDemo {
    private static final Logger logger = LoggerFactory.getLogger(Dispatcher.class);

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        InputStream inputStream = new FileInputStream(new File(MultiThreadAppConfig.propertiesFile));
        properties.load(inputStream);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.CLIENT_ID_CONFIG, MultiThreadAppConfig.applicationId);
        KafkaProducer<Integer, String> producer = new KafkaProducer<>(properties);
        Thread[] threads = new Thread[MultiThreadAppConfig.eventFiles.length];
        for (int i = 0; i < MultiThreadAppConfig.eventFiles.length; i++) {
            threads[i] = new Thread(new Dispatcher(MultiThreadAppConfig.eventFiles[i], MultiThreadAppConfig.topicname, producer));
            threads[i].start();
        }

        try {
            for (Thread t : threads) t.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }

}
