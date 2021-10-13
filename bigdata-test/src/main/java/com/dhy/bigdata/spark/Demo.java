package com.dhy.bigdata.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * @Project cola-boss
 * @Description 主要用途描述
 * @Author lvaolin
 * @Date 2021/10/13 下午4:17
 */
public class Demo {

    public static void main(String[] args) {
        // Creates a DataFrame based on a table named "people"
        // stored in a MySQL database.
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("mysql");
        conf.set("spark.sql.shuffle.partitions", "1");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        String url = "jdbc:mysql://127.0.0.1:3306/ttk_data_test_0006?useUnicode=true&characterEncoding=UTF8&allowMultiQueries=true";
        Dataset<Row> df = sqlContext
                .read()
                .format("jdbc")
                .option("url", url)
                .option("user","root")
                .option("password","root")
                .option("dbtable", "edf_org")
                .load();

        // Looks the schema of this DataFrame.  打印所有企业基本信息

        df.printSchema();
        df.show();

//        df.foreach((row)->{
//            System.out.println(row.prettyJson());
//        });

        // Counts edf_org by accountingStandards 按会计准则进行分类统计
        Dataset<Row> countsByAccountingStandards = df.groupBy("accountingStandards").count();
        countsByAccountingStandards.show();

        // Saves countsByAge to S3 in the JSON format. 将统计结果以json格式保存到另一个地方
        //countsByAccountingStandards.write().format("json").save("s3a://...");

        while (true);
    }
}
