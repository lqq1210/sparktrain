package com.xidian.spark.project.domain

/**
  * @author YuZhansheng
  * @desc 清洗后的日志信息
  * @param日志访问的ip地址
  * @param日志访问的时间
  * @param日志访问的实战课程编号
  * @param日志访问的状态码
  * @param日志访问的referer
  * @create 2019-02-26 15:43
  */
case class ClickLog(ip:String, time:String, courseId:Int, statusCode:Int, referer:String)
