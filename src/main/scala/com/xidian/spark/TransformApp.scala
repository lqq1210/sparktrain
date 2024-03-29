package com.xidian.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author YuZhansheng
  * @desc  黑名单过滤
  * @create 2019-02-20 15:33
  */
object TransformApp {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[2]").setAppName("TransformApp")

        //创建StreamingContext需要两个参数：SparkConf和batch interval
        val ssc = new StreamingContext(sparkConf,Seconds(5))

        //构建黑名单
        val blacks = List("zhangsan","lisi")
        val blacksRDD = ssc.sparkContext.parallelize(blacks).map(x=>(x,true))

        val lines = ssc.socketTextStream("localhost",6789)

        val clicklog = lines.map(x => (x.split(",")(1),x)).transform(rdd => {
            rdd.leftOuterJoin(blacksRDD)
                .filter(x => x._2._2.getOrElse(false) != true)
                .map(x => x._2._1)
        })

        clicklog.print()

        ssc.start()
        ssc.awaitTermination()
    }
}
