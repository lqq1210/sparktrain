//package com.xidian.spark
//
//import org.apache.kafka.clients.consumer.ConsumerRecord
//import org.apache.kafka.common.serialization.StringDeserializer
//import org.apache.spark.SparkConf
//import org.apache.spark.streaming.dstream.{DStream, InputDStream}
//import org.apache.spark.streaming.kafka010.KafkaUtils
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
//import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
//
///**
//  * @author YuZhansheng
//  * @desc  Spark Streaming对接kafka的方式2——基于Direct的整合
//  * @create 2019-02-23 11:28
//  */
//object KafkaDirectWordCount {
//
//    def main(args: Array[String]): Unit = {
//
//        val sparkConf = new SparkConf().setAppName("KafkaDirectWordCount").setMaster("local[2]")
//
//        val ssc = new StreamingContext(sparkConf,Seconds(5))
//
//        val topics:String = "kafka_streaming_topic"
//        val topicarr = topics.split(",")
//
//        val brokers = "hadoop0:9092,hadoop1:9092,hadoop2:9092,hadoop3:9092"
//
//        val kafkaParams:Map[String,Object] = Map[String,Object](
//            "bootstrap.servers" -> brokers,
//            "key.deserializer" -> classOf[StringDeserializer],
//            "value.deserializer" -> classOf[StringDeserializer],
//            "group.id" -> "test",
//            "auto.offset.reset" -> "latest",
//            "enable.auto.commit" -> (false: java.lang.Boolean)
//        )
//
//        val kafka_streamDStream: InputDStream[ConsumerRecord[String,String]] = KafkaUtils.createDirectStream(
//            ssc,PreferConsistent,
//            Subscribe[String,String](topicarr,kafkaParams))
//
//        val resDStream: DStream[((Long, Int, String), Int)] = kafka_streamDStream.map(line =>
//            (line.offset(), line.partition(), line.value())).
//            flatMap(t =>{t._3.split(" ").map(word => (t._1,t._2,word))}).
//            map(k => ((k._1,k._2,k._3),1)).reduceByKey(_ + _)
//
//        resDStream.print()
//
//        ssc.start()
//        ssc.awaitTermination()
//    }
//}
