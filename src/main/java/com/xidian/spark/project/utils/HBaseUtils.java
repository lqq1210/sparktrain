package com.xidian.spark.project.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author YuZhansheng
 * @desc  HBase操作工具类，Java工具类建议采用单例模式封装
 * @create 2019-02-27 10:11
 */
public class HBaseUtils {

    HBaseAdmin admin = null;
    Configuration configuration = null;

    //单例模式需要私有构造方法
    private HBaseUtils(){
        configuration = new Configuration();
        configuration.set("hbase.zookeeper.quorum","hadoop0,hadoop1,hadoop2,hadoop3");
        configuration.set("hbase.rootdir","hdfs://hadoop0:9000/hbase");

        try {
            admin = new HBaseAdmin(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //懒汉式单例模式
    private static HBaseUtils instance = null;
    public static synchronized HBaseUtils getInstance(){
        if (null == instance){
            instance = new HBaseUtils();
        }
        return instance;
    }

    //根据表名获取到HTable实例
    public HTable getTable(String tableName){
        HTable table = null;
        try {
            table = new HTable(configuration, tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return table;
    }

    /**
     * 添加一条记录到HBase表
     * @param tableName HBase表名
     * @param rowkey  HBase表的rowkey
     * @param cf HBase表的columnfamily
     * @param column HBase表的列
     * @param value  写入HBase表的值
     */
    public void put(String tableName,String rowkey,String cf,String column,String value){
        HTable table = getTable(tableName);

        Put put = new Put(Bytes.toBytes(rowkey));
        put.add(Bytes.toBytes(cf), Bytes.toBytes(column), Bytes.toBytes(value));

        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

//    //测试数据，使用时将这个主函数注释掉
//    public static void main(String[] args) {
//
//        //HTable table = HBaseUtils.getInstance().getTable("imooc_course_clickcount");
//        //System.out.println(table.getName().getNameAsString());
//
//        String tableName = "imooc_course_clickcount" ;
//        String rowkey = "20171111_88";
//        String cf = "info" ;
//        String column = "click_count";
//        String value = "2";
//
//        HBaseUtils.getInstance().put(tableName, rowkey, cf, column, value);
//    }
}
