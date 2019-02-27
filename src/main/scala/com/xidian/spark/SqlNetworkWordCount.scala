package com.xidian.spark

import java.beans.Transient

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

/**
  * @author YuZhansheng
  * @desc Spark Streaming整合SparkSQL 完成词频统计操作
  * @create 2019-02-20 16:03
  */
object SqlNetworkWordCount {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[2]").setAppName("SqlNetworkWordCount")

        //创建StreamingContext需要两个参数：SparkConf和batch interval
        val ssc = new StreamingContext(sparkConf,Seconds(5))

        val lines = ssc.socketTextStream("localhost",6789)
        val words = lines.flatMap(_.split(" "))

        words.foreachRDD { (rdd:RDD[String],time:Time) =>

            val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)

            import spark.implicits._

            val wordsDataFrame = rdd.map(w => Record(w)).toDF()

            wordsDataFrame.createOrReplaceTempView("words")

            val wordCountsDataFrame = spark.sql("select word, count(*) as total from words group by word")

            println(s"======= $time =======")

            wordCountsDataFrame.show()
        }

        ssc.start()
        ssc.awaitTermination()
    }

    case class Record(word:String)

    object SparkSessionSingleton{

        @Transient private var instance:SparkSession = _

        def getInstance(sparkConf: SparkConf):SparkSession = {
            if (instance == null){
                instance = SparkSession
                    .builder()
                    .config(sparkConf)
                    .getOrCreate()
            }
            instance
        }
    }
}
