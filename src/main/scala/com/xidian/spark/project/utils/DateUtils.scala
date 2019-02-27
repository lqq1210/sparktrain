package com.xidian.spark.project.utils

import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat

/**
  * @author YuZhansheng
  * @desc 日期时间工具类
  * @create 2019-02-26 15:22
  */
object DateUtils {

    val YYYYMMDDHHMMSS_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

    val TARGE_FORMAT = FastDateFormat.getInstance("yyyyMMddHHmmss")

    def getTime(time:String) = {
        YYYYMMDDHHMMSS_FORMAT.parse(time).getTime
    }

    def parseToMinute(time:String) = {
        TARGE_FORMAT.format(new Date(getTime(time)))
    }

    def main(args: Array[String]): Unit = {
        println(parseToMinute("2019-02-26 15:22:01"))
    }

}
