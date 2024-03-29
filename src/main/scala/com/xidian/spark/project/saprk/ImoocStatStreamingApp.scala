package com.xidian.spark.project.saprk

import com.xidian.spark.project.dao.{CourseClickCountDAO, CourseSearchClickCountDAO}
import com.xidian.spark.project.domain.{ClickLog, CourseClickCount, CourseSearchClickCount}
import com.xidian.spark.project.utils.DateUtils
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

/**
  * @author YuZhansheng
  * @desc 使用SparkStreaming处理Kafka过来的数据
  * @create 2019-02-26 11:40
  */
object ImoocStatStreamingApp {

    def main(args: Array[String]): Unit = {

        //判断参数个数是否为4，不为4则退出运行
        if(args.length != 4){
            println("Usage:ImoocStatStreamingApp <zkQuorum> <group> <topics> <numThreads>")
            System.exit(1)
        }

        val Array(zkQuorum,groupId,topics,numThreads) = args

        val sparkConf = new SparkConf().setAppName("ImoocStatStreamingApp").setMaster("local[2]")
        val ssc = new StreamingContext(sparkConf,Seconds(60))

        val topicMap = topics.split(",").map((_,numThreads.toInt)).toMap

        val messages = KafkaUtils.createStream(ssc,zkQuorum,groupId,topicMap)

        //测试步骤一：测试数据接收
        messages.map(_._2).count().print

        //测试步骤二：数据清洗
        val logs = messages.map(_._2)
        val cleanData = logs.map(line => {
            val infos = line.split("\t")
            //infos(2) = "GET /class/112.html HTTP/1.1"
            //url = /class/112.html
            val url = infos(2).split(" ")(1)
            var courseId = 0
            //获取以/class开头的课程的编号
            if (url.startsWith("/class")){
                val courseIdHTML = url.split("/")(2)
                courseId = courseIdHTML.substring(0,courseIdHTML.lastIndexOf(".")).toInt
            }
            ClickLog(infos(0),DateUtils.parseToMinute(infos(1)),courseId,infos(3).toInt,infos(4))
        }).filter(clicklog => clicklog.courseId != 0)

        //cleanData.print()
        //测试步骤三：统计到今天为止，实战课程（以/class开头）的访问量
        cleanData.map(x => {
            //HBase rowkey设计：20190226_8
            (x.time.substring(0,8) + "_" + x.courseId,1)
        }).reduceByKey(_ + _).foreachRDD(rdd => {
            rdd.foreachPartition(partitionRecords => {
                val list = new ListBuffer[CourseClickCount]
                partitionRecords.foreach(pair => {
                    list.append(CourseClickCount(pair._1,pair._2))
                })

                CourseClickCountDAO.save(list)
            })
        })

        //测试步骤四：统计从搜索引擎过来的今天到现在为止实战课程的访问量
        cleanData.map(x => {

            // 转换：https://www.sogou.com/web?query=Spark SQL实战==>https:/www.sogou.com/web?query=Spark SQL实战
            val referer = x.referer.replaceAll("//", "/")

            val splits = referer.split("/")
            var host = ""
            if(splits.length > 2) {
                host = splits(1)
            }
            (host, x.courseId, x.time)
        }).filter(_._1 != "").map(x => {
            (x._3.substring(0,8) + "_" + x._1 + "_" + x._2 , 1)
        }).reduceByKey(_ + _).foreachRDD(rdd => {
            rdd.foreachPartition(partitionRecords => {
                val list = new ListBuffer[CourseSearchClickCount]

                partitionRecords.foreach(pair => {
                    list.append(CourseSearchClickCount(pair._1, pair._2))
                })
                CourseSearchClickCountDAO.save(list)
            })
        })

        ssc.start()
        ssc.awaitTermination()
    }
}
