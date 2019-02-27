package com.xidian.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author YuZhansheng
  * @desc Spark Streaming处理文件系统数据（包括HDFS和本地文件系统）
  * @create 2019-02-19 21:31
  */
object FileWordCount {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local").setAppName("FileWordCount")
        val ssc = new StreamingContext(sparkConf,Seconds(5))

        //监控/root/DataSet这个文件下文件内容
        val lines = ssc.textFileStream("/root/DataSet")

        val result = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

        result.print()

        ssc.start()
        ssc.awaitTermination()
    }
}
