package com.xidian.spark.kafka;

/**
 * @author YuZhansheng
 * @desc  Kafka Java API测试
 * @create 2019-02-17 16:32
 */
public class KafkaClientApp {

    public static void main(String[] args) {
        new KafkaProducer(KafkaProperties.TOPIC).start();

        new KafkaConsumer(KafkaProperties.TOPIC).start();
    }
}
