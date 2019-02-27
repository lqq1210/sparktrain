package com.xidian.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author YuZhansheng
  * @desc SparkStreaming处理socket数据
  * @create 2019-02-19 11:26
  */
object NetworkWordCount {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")

    //创建StreamingContext需要两个参数：SparkConf和batch interval
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    val lines = ssc.socketTextStream("localhost",6789)

    val result = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    result.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
