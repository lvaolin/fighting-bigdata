package com.dhy.bigdata.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @Project cola-boss
 * @Description 使用spark联查过滤两个表的数据，将计算结果保存到第3张表中
 * @Author lvaolin
 * @Date 2021/10/13 下午3:55
 */
public class SparkMysql002 {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("mysql");
        /**
         * 配置join或者聚合操作shuffle数据时分区的数量
         */
        conf.set("spark.sql.shuffle.partitions", "1");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        /**
         * 加载第一个表的数据
         */
        Map<String, String> options = new HashMap<String, String>();
        options.put("url", "jdbc:mysql://127.0.0.1:3306/ttk_data_test_0006?useUnicode=true&characterEncoding=UTF8&allowMultiQueries=true");
        options.put("driver", "com.mysql.jdbc.Driver");
        options.put("user", "root");
        options.put("password", "root");
        options.put("dbtable", "es_person");

        Dataset<Row> es_person = sqlContext.read().format("jdbc").options(options).load();
        es_person.show();
        es_person.registerTempTable("es_person");


        /**
         * 加载第2个表的数据
         */

        options.put("dbtable", "es_department");
        Dataset<Row> es_department = sqlContext.read().format("jdbc").options(options).load();
        es_department.show();
        es_department.registerTempTable("es_department");

        /**
         * 联查两个表的数据
         */
        Dataset<Row> result =
                sqlContext.sql("select es_person.agencyId,es_person.id,es_person.name as personName,es_department.name as departmentName  from es_person left join es_department on es_person.departmentId = es_department.id ");
        result.show();

        /**
         * 将统计结果保存到Mysql中
         */
        Properties properties = new Properties();
        properties.setProperty("user", "root");
        properties.setProperty("password", "root");
        /*
         * SaveMode:
         * Overwrite：覆盖
         * Append:追加
         * ErrorIfExists:如果存在就报错
         * Ignore:如果存在就忽略
         */
        result.write().mode(SaveMode.Overwrite).jdbc("jdbc:mysql://127.0.0.1:3306/ttk_data_test_0006", "spark_result1", properties);
        System.out.println("----Finish----");
        sc.stop();

    }
}
