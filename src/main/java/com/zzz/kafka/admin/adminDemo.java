package com.zzz.kafka.admin;

import org.apache.kafka.clients.admin.*;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class adminDemo {

    private static final String TOPIC_NAME = "zzz_test_topic";


    public static void main(String[] args) throws ExecutionException, InterruptedException {
//        AdminClient adminClient = adminClient();
//        System.out.println("adminClient:"+ adminClient);
//        createTopic();
        viewTopics();
//        deleteTopics();
//        viewTopics();
    }

    //客户端
    public static AdminClient adminClient() {
        Properties properties = new Properties();
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "116.205.168.85:9092");
        AdminClient adminClient = AdminClient.create(properties);
        return adminClient;
    }

    //创建topic
    public static void createTopic() {
        AdminClient adminClient = adminClient();
        Short r = 1;
        NewTopic newTopic = new NewTopic(TOPIC_NAME, 2, r);
        CreateTopicsResult topics = adminClient.createTopics(Arrays.asList(newTopic));
        System.out.println(topics);

    }

    //查看Topic
    public static void viewTopics() throws ExecutionException, InterruptedException {
        AdminClient adminClient = adminClient();
        ListTopicsOptions listTopicsOptions = new ListTopicsOptions();
        listTopicsOptions.listInternal(true);
//        listTopicsOptions.timeoutMs(10000);
//
//
        ListTopicsResult listTopicsResult = adminClient.listTopics(listTopicsOptions);
//        ListTopicsResult listTopicsResult = adminClient.listTopics();
        Set<String> strings = listTopicsResult.names().get();
        strings.forEach(System.out::println);

    }

    //删除TOPIC
    public static void deleteTopics() throws ExecutionException, InterruptedException {
        AdminClient adminClient = adminClient();
        adminClient.deleteTopics(Collections.singleton(TOPIC_NAME));

    }

}
