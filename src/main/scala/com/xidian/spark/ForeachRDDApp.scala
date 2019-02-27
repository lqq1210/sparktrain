package com.xidian.spark

import java.sql.DriverManager

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @ author YuZhansheng
  * @ desc 使用Spark Streaming完成词频统计，并将结果写入到MySQL中
  * @ create 2019-02-20 11:04
  */
object ForeachRDDApp {
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setAppName("ForeachRDDApp").setMaster("local[2]")

        val ssc = new StreamingContext(sparkConf,Seconds(5))

        val lines = ssc.socketTextStream("localhost",6789)

        val result = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

        //result.print()  //此处仅仅是将结果统计输出在控制台

        //TODO 将结果写入到mysql
//        result.foreachRDD(rdd => {
//            val connection = createConnection()
//            rdd.foreach{ record =>
//                val sql = "insert into wordcount(word, wordcount) values('"+record._1+"',"+record._2 +")"
//                connection.createStatement().execute(sql)
//            }
//        })  会出现序列化异常

        result.foreachRDD(rdd => {
            rdd.foreachPartition(partitionOfRecords => {
                val connection = createConnection()
                partitionOfRecords.foreach(record => {
                    val sql = "insert into wordcount(word, wordcount) values('"+record._1+"',"+record._2 +")"
                    connection.createStatement().execute(sql)
                })
                connection.close()
            })
        })

        ssc.start()
        ssc.awaitTermination()
    }

    //获取MySQL的连接
    def createConnection() = {
        Class.forName("com.mysql.jdbc.Driver")
        DriverManager.getConnection("jdbc:mysql://localhost:3306/spark","root","18739548870yu")
    }
}
