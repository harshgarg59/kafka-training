package com.inn.config;

public class MultiThreadAppConfig {
    public final static String applicationId = "Mutli-Threaded-Producer";
    public final static String kafkabroker = "localhost:9092,localhost:9093,localhost:9094";
    public final static String topicname = "nse-eod-topic";
    public final static String propertiesFile = "src/main/resources/kafka.properties";
    public final static String[] eventFiles = {"src/data/NSE05NOV2018BHAV.csv", "src/data/NSE06NOV2018BHAV.csv"};
}
