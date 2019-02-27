package com.xidian.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author YuZhansheng
  * @desc  Spark Streaming对接kafka的方式1——基于Receiver的整合
  * @create 2019-02-23 11:28
  */
object KafkaReceiverWordCount {

    def main(args: Array[String]): Unit = {

        //参数通过IDEA传入
        if (args.length != 4){
            System.err.println("Usage:KafkaReceiverWordCount <zkQuorum> <group> <topics> <numThreads>")
        }

        val Array(zkQuorum,group,topics,numThreads) = args

        val sparkConf = new SparkConf().setAppName("KafkaReceiverWordCount").setMaster("local[2]")

        val ssc = new StreamingContext(sparkConf,Seconds(5))

        val topicMap = topics.split(",").map((_,numThreads.toInt)).toMap

        //TODO ..SparkStreaming对接Kafka
        //val kafkaStream = KafkaUtils.createStream(streamingContext,[ZK quorum], [consumer group id], [per-topic number of Kafka partitions to consume])
        val messages = KafkaUtils.createStream(ssc,zkQuorum,group,topicMap)

        messages.map(_._2).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()

        ssc.start()
        ssc.awaitTermination()
    }
}
