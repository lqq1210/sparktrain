package com.xidian.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author YuZhansheng
  * @desc Spark Streaming与Flume的整合-push方式
  * @create 2019-02-21 10:04
  */
object FlumePushWordCount {

    def main(args: Array[String]): Unit = {

        //判断参数
        if(args.length != 2){
            System.err.println("Usage:FlumePushWordCount <hostname> <port>")
            System.exit(1)
        }

        //通过参数传递主机名和端口号
        val Array(hostname,port) = args

        val sparkConf = new SparkConf().setMaster("local[2]").setAppName("FlumePushWordCount")
        val ssc = new StreamingContext(sparkConf,Seconds(5))

        //TODO...如何使用SparkStreaming整合Flume
        val flumeStream = FlumeUtils.createStream(ssc,hostname,port.toInt)

        flumeStream.map(x => new String(x.event.getBody.array()).trim)
            .flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()

        ssc.start()
        ssc.awaitTermination()
    }
}
