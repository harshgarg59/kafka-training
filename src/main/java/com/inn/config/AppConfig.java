package com.inn.config;

public class AppConfig {
    public final static String applicationId = "HelloProducer";
    public final static String kafkabroker = "localhost:9092,localhost:9093,localhost:9094";
    public final static String topicname = "hello-producer";
    public final static int numEvents = 100000;
}
