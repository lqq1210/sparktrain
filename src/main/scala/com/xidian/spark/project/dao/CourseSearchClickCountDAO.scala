package com.xidian.spark.project.dao

import com.xidian.spark.project.domain.CourseSearchClickCount
import com.xidian.spark.project.utils.HBaseUtils
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

/**
  * 从搜索引擎过来的实战课程点击数-数据访问层
  */
object CourseSearchClickCountDAO {

  val tableName = "imooc_course_search_clickcount"
  val cf = "info"
  val qualifer = "click_count"


  /**
    * 保存数据到HBase
    * @param list  CourseSearchClickCount集合
    */
  def save(list: ListBuffer[CourseSearchClickCount]): Unit = {

    val table = HBaseUtils.getInstance().getTable(tableName)

    for(ele <- list) {
      table.incrementColumnValue(Bytes.toBytes(ele.day_search_course),
        Bytes.toBytes(cf),
        Bytes.toBytes(qualifer),
        ele.click_count)
    }
  }


  /**
    * 根据rowkey查询值
    */
  def count(day_search_course: String):Long = {
    val table = HBaseUtils.getInstance().getTable(tableName)

    val get = new Get(Bytes.toBytes(day_search_course))
    val value = table.get(get).getValue(cf.getBytes, qualifer.getBytes)

    if(value == null) {
      0L
    }else{
      Bytes.toLong(value)
    }
  }
//
//  def main(args: Array[String]): Unit = {
//
//    //测试可用否
//    val list = new ListBuffer[CourseSearchClickCount]
//    list.append(CourseSearchClickCount("20190227_www.baidu.com_8",8))
//    list.append(CourseSearchClickCount("20190227_cn.bing.com_9",9))
//
//    save(list)
//
//    println(count("20190227_www.baidu.com_8") + " : " + count("20190227_cn.bing.com_9"))
//  }

}
