package com.xidian.spark.project.dao

import com.xidian.spark.project.domain.CourseClickCount
import com.xidian.spark.project.utils.HBaseUtils
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

/**
  * @author YuZhansheng
  * @desc  实战课程点击数的数据访问层
  * @create 2019-02-27 9:51
  */
object CourseClickCountDAO {

    val tableName = "imooc_course_clickcount"
    val cf = "info"

    val qualifer = "click_count"

    //保存数据到HBase
    def save(list:ListBuffer[CourseClickCount]):Unit = {
        val table = HBaseUtils.getInstance().getTable(tableName)
        for (ele <- list){
            table.incrementColumnValue(Bytes.toBytes(ele.day_course),
                Bytes.toBytes(cf),
                Bytes.toBytes(qualifer),
                ele.click_count
            )
        }
    }

    //根据rowkey查询值
    def count(day_course:String):Long = {
        val table = HBaseUtils.getInstance().getTable(tableName)

        val get = new Get(Bytes.toBytes(day_course))
        val value = table.get(get).getValue(cf.getBytes,qualifer.getBytes)

        if (value == null){
            0l
        }else{
            Bytes.toLong(value)
        }
    }

    //测试程序是否可用
//    def main(args: Array[String]): Unit = {
//
//        val list = new ListBuffer[CourseClickCount]
//        list.append(CourseClickCount("20190227_8",8))
//        list.append(CourseClickCount("20190227_9",18))
//        list.append(CourseClickCount("20190227_1",12))
//
//        save(list)
//    }

}
