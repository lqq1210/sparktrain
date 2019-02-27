package com.xidian.spark.project.domain

/**
  * @author YuZhansheng
  * @desc 实战课程访问数
  *         day_course:对应的是HBase中的rowkey,格式：20190227_1
  *         click_count:对应的是20190227_1这一天该课程的访问数
  * @create 2019-02-26 19:19
  */
case class CourseClickCount (day_course:String,click_count:Long)