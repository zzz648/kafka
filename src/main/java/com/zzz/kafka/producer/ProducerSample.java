package com.zzz.kafka.producer;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class ProducerSample {
    private static final String TOPIC_NAME = "zzz-topic";

    public static void main(String[] args) {
        producerSend();
    }


    //producer 发送消息
    private static void producerSend() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "165.154.104.175:9092");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "0");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
        properties.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        // 设置连接超时时间
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000); // 5秒
        properties.put(ProducerConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG, 10000); // 5秒
        properties.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 10000); // 5秒
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 20000); // 5秒
        KafkaProducer producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; i++) {
            //消息对象
            ProducerRecord producerRecord = new ProducerRecord(TOPIC_NAME, "name" + i, "value" + i);
            producer.send(producerRecord);
        }
        producer.close();
    }
}
