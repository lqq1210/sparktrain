package com.xidian.spark.kafka;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @author YuZhansheng
 * @desc   消费者
 * @create 2019-02-17 16:39
 */
public class KafkaConsumer extends Thread{

    private String topic;

    public KafkaConsumer(String topic){
        this.topic = topic;
    }

    private ConsumerConnector createConnector(){
        Properties properties = new Properties();
        properties.put("zookeeper.connect",KafkaProperties.ZK);
        properties.put("group.id",KafkaProperties.GROUP_ID);

        return Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));
    }

    @Override
    public void run() {
        ConsumerConnector consumer = createConnector();

        Map<String,Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic,1);

        //第一个String就是传的topic；List<KafkaStream就是对应的数据流
        Map<String, List<KafkaStream<byte[], byte[]>>> messageStream = consumer.createMessageStreams(topicCountMap);

        //get(0)就是获取每次接收到的数据
        KafkaStream<byte[], byte[]> stream = messageStream.get(topic).get(0);

        ConsumerIterator<byte[], byte[]> iterator = stream.iterator();

        while (iterator.hasNext()){
            String message = new String(iterator.next().message());
            System.out.println("rec: " + message);
        }
    }
}
