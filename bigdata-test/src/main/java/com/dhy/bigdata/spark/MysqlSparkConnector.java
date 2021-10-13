//package com.dhy.bigdata.spark;
//
//import com.alibaba.fastjson.JSONArray;
//import com.alibaba.fastjson.JSONObject;
//import org.apache.log4j.Logger;
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Row;
//import org.apache.spark.sql.SparkSession;
//
//import java.io.InputStream;
//import java.util.Arrays;
//import java.util.Iterator;
//import java.util.List;
//import java.util.Properties;
//
//public class MysqlSparkConnector {
//
//    private static Logger logger = Logger.getLogger("MysqlSparkConnector.class");
//
//    private static String confilePrefix = "druid.properties";
//
//    private static String driver = null;
//    private static String dbHost = null;
//    private static String dbPort = null;
//    private static String username = null;
//    private static String password = null;
//
//    private static String environmentArray[] = { "development", "query", "master", "sandbox" };
//
//    /**
//     * 用Spark从MySQL中查询数据
//     *      可在同一个Mysql数据库中联表查询
//     *
//     * @param spark
//     * @param dbEnv
//     * @param dbName
//     * @param tables
//     * @param sql
//     * @return
//     */
//    public static Dataset<Row> queryBySparkFromMysql(SparkSession spark, String dbEnv, String dbName, String[] tables,
//                                                     String sql) {
//
//        if (!readProperties(dbEnv, dbName)) {
//            logger.error("read properties error, please check configuration file: " + dbName + "." + dbEnv + "." + confilePrefix);
//            return null;
//        }
//
//        String url = "jdbc:mysql://" + dbHost + ":" + dbPort + "/" + dbName;
//
//        Properties connectionProperties = new Properties();
//        connectionProperties.put("driver", driver);
//        connectionProperties.put("user", username);
//        connectionProperties.put("password", password);
//
//        for (int i = 0; i < tables.length; i++) {
//            spark.read().jdbc(url, tables[i], connectionProperties).createOrReplaceTempView(tables[i]);
//        }
//
//        return spark.sql(sql);
//    }
//
//    /**
//     * 用Spark从MySQL中查询数据
//     *      分片查询数据量大的表
//     *
//     * @param spark
//     * @param dbEnv
//     * @param dbName
//     * @param table
//     * @param sql
//     * @param partitionColumn   用于分片的字段，必须是整数类型
//     * @param lowerBound        分片的下界
//     * @param upperBound        分片的上界
//     * @param numPartitions     要创建的分片数
//     * @return
//     */
//    public static Dataset<Row> queryBySparkFromMysql(SparkSession spark, String dbEnv, String dbName, String[] tables,
//            String sql, String partitionColumn, long lowerBound, long upperBound, int numPartitions) {
//
//        if (!readProperties(dbEnv, dbName)) {
//            logger.error("read properties error, please check configuration file: " + dbName + "." + dbEnv + "." + confilePrefix);
//            return null;
//        }
//
//        String url = "jdbc:mysql://" + dbHost + ":" + dbPort + "/" + dbName;
//
//        Properties connectionProperties = new Properties();
//        connectionProperties.put("driver", driver);
//        connectionProperties.put("user", username);
//        connectionProperties.put("password", password);
//
//        /**
//         * Spark默认的jdbc并发度为1，所有的数据都会在一个partition中进行操作，无论你给的资源有多少，只有会有一个task在执行任务，如果查询的表比较大的话，很容易就会内存溢出
//         * 所以当数据量达到百万甚至千万以上时，就需要对数据源进行分片查询，即多个进程同时查询部分数据，避免一次性查出的数据过多导致内存溢出
//         *      如：
//         *          partitionColumn : id
//         *          lowerBound : 0
//         *          upperBound : 10000
//         *          numPartitions : 10
//         *      只会找出(upperBound-lowerBound)=10000条记录，根据id字段分numPartitions=10次查询，每次找出(upperBound-lowerBound)/numPartitions=1000条记录
//         *          select * from table where id between 0 and 1000
//         *          select * from table where id between 1000 and 2000
//         *              ......
//         *          select * from table where id between 9000 and 10000
//         */
//
//        for (int i = 0; i < tables.length; i++) {
//            spark.read().jdbc(url, tables[i], partitionColumn, lowerBound, upperBound, numPartitions, connectionProperties).createOrReplaceTempView(tables[i]);
//        }
//
//        return spark.sql(sql);
//    }
//
//    /**
//     * 用Spark从MySQL中查询数据
//     *      可连接多个Mysql数据库后联表查询
//     *
//     *          {
//     *              "cets_swapdata": [
//     *                  {
//     *                      "isPartition": true,
//     *                      "partitionInfo": {
//     *                          "upperBound": 500000000,
//     *                          "numPartitions": 100,
//     *                          "partitionColumn": "id",
//     *                          "lowerBound": 0
//     *                      },
//     *                      "table": "fba_inventory"
//     *                  }
//     *              ],
//     *              "cets": [
//     *                  {
//     *                      "isPartition": false,
//     *                      "table": "stock_location"
//     *                  }
//     *              ]
//     *          }
//     *
//     * @param spark
//     * @param dbEnv
//     * @param dbTableJson
//     * @param sql
//     * @return
//     * @throws JSONException
//     */
//    public static Dataset<Row> jointQueryBySparkFromMysql(SparkSession spark, String dbEnv, JSONObject dbTableJson,
//            String sql) {
//
//        String url = null;
//        Properties connectionProperties = null;
//        String dbName = null;
//
//        JSONArray tablesJSONArray = null;
//        JSONObject tableInfo = null;
//
//        String tableName = null;
//        boolean isPartition = false;
//        JSONObject partitionInfo = null;
//
//        String partitionColumn = null;
//        long lowerBound = 0;
//        long upperBound = 100;
//        int numPartitions = 1;
//
//        try {
//            @SuppressWarnings("unchecked")
//            Iterator<String> iterator = dbTableJson.keys();
//            while (iterator.hasNext()) {
//                dbName = iterator.next();
//                tablesJSONArray = dbTableJson.getJSONArray(dbName);
//
//                if (!readProperties(dbEnv, dbName)) {
//                    logger.error("read properties error, please check configuration file: " + dbName + "." + dbEnv + "." + confilePrefix);
//                    return null;
//                }
//
//                url = "jdbc:mysql://" + dbHost + ":" + dbPort + "/" + dbName;
//
//                connectionProperties = new Properties();
//                connectionProperties.put("driver", driver);
//                connectionProperties.put("user", username);
//                connectionProperties.put("password", password);
//
//                for (int i = 0; i < tablesJSONArray.length(); i++) {
//                    tableInfo = tablesJSONArray.getJSONObject(i);
//
//                    tableName = tableInfo.getString("table");
//                    isPartition = tableInfo.getBoolean("isPartition");
//                    if (isPartition) {
//                        partitionInfo = tableInfo.getJSONObject("partitionInfo");
//
//                        // 数据量大的表，分片读取
//                        partitionColumn = partitionInfo.getString("partitionColumn");
//                        lowerBound = partitionInfo.getLong("lowerBound");
//                        upperBound = partitionInfo.getLong("upperBound");
//                        numPartitions = partitionInfo.getInt("numPartitions");
//
//                        spark.read().jdbc(url, tableName, partitionColumn, lowerBound, upperBound, numPartitions, connectionProperties).createOrReplaceTempView(tableName);
//
//                    } else {
//                        // 数据量小的表，一次读取
//                        spark.read().jdbc(url, tableName, connectionProperties).createOrReplaceTempView(tableName);
//                    }
//                }
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//        return spark.sql(sql);
//    }
//
//    /**
//     * 用Spark从MySQL中查询数据
//     *      可以根据任意字段分片查询
//     *
//     * @param spark
//     * @param dbEnv
//     * @param dbName
//     * @param tableName
//     * @param predicates    查询条件，每个条件都是一个task，即分片数==数组长度
//     * @return
//     */
//    public static Dataset<Row> queryBySparkFromMysqlByConditions(SparkSession spark, String dbEnv, String dbName,
//            String tableName, String[] predicates) {
//
//        if (!readProperties(dbEnv, dbName)) {
//            logger.error("read properties error, please check configuration file: " + dbName + "." + dbEnv + "." + confilePrefix);
//            return null;
//        }
//
//        String url = "jdbc:mysql://" + dbHost + ":" + dbPort + "/" + dbName;
//
//        Properties connectionProperties = new Properties();
//        connectionProperties.put("driver", driver);
//        connectionProperties.put("user", username);
//        connectionProperties.put("password", password);
//
//        /**
//         * 根据查询条件predicates分片进行查询，如根据时间类型的字段modifiedOn，一个task查询一天的数据：
//         *      String[] predicates = {
//         *          "modifiedOn >= '2017-09-02' and modifiedOn < '2017-09-03'",
//         *          "modifiedOn >= '2017-09-03' and modifiedOn < '2017-09-04'",
//         *          "modifiedOn >= '2017-09-04' and modifiedOn < '2017-09-05'",
//         *              ......
//         *          "modifiedOn >= '2017-09-17' and modifiedOn < '2017-09-18'",
//         *          "modifiedOn >= '2017-09-18' and modifiedOn < '2017-09-19'"
//         *      }
//         *
//         *  也可以根据多个字段分片查询，就像是SQL语句中的where条件一样：
//         *      String[] predicates = {
//         *          "modifiedOn >= '2017-09-02' and modifiedOn < '2017-09-23'",
//         *          "modifiedOn >= '2018' and modifiedOn < '2019'",
//         *          "id < 998",
//         *          "skuId = 'a17052200ux1459'"
//         *      }
//         *
//         *  注意：如果条件重复的话，是会查出重复数据来的，如：
//         *      String[] predicates = {"id=1", "id=2", "id=2"}
//         *      这样会找出三条记录，其中两条id=2的记录是一模一样的
//         */
//
//        return spark.read().jdbc(url, tableName, predicates, connectionProperties);
//    }
//
//    /**
//     *
//     * 读取对应MySQL的配置信息
//     *
//     * @param environment
//     * @param dbName
//     * @return
//     */
//    private static boolean readProperties(String environment, String dbName) {
//        String errorMsg = "";
//        boolean isContinue = true;
//
//        String confile = null;
//        InputStream is = null;
//
//        try {
//            // environment
//            List<String> environmentList = Arrays.asList(environmentArray);
//            if (!environmentList.contains(environment)) {
//                errorMsg = "environment must be one of: " + environmentList.toString();
//                isContinue = false;
//            }
//
//            // dbName
//            if (CommonFunction.isEmptyOrNull(dbName)) {
//                errorMsg = "dbName can not be null";
//                isContinue = false;
//            }
//
//            // 读取配置文件
//            if (isContinue) {
//                confile = dbName + "." + environment + "." + confilePrefix;
//                is = MysqlSparkConnector.class.getClassLoader().getResourceAsStream(confile);
//
//                if (is == null) {
//                    errorMsg = "resource file: [" + confile + "] not find.";
//                    isContinue = false;
//                }
//
//                if (isContinue) {
//                    Properties properties = new Properties();
//                    properties.load(is);
//
//                    driver = properties.getProperty("driverClassName");
//                    dbHost = properties.getProperty("dbHost");
//                    dbPort = properties.getProperty("dbPort");
//                    username = properties.getProperty("username");
//                    password = properties.getProperty("password");
//
//                    if (isContinue) {
//                        if (CommonFunction.isEmptyOrNull(driver)) {
//                            errorMsg = "the driver can not be null, please set in confile: " + confile;
//                            isContinue = false;
//                        }
//                    }
//
//                    if (isContinue) {
//                        if (CommonFunction.isEmptyOrNull(dbHost)) {
//                            errorMsg = "the dbHost can not be null, please set in confile: " + confile;
//                            isContinue = false;
//                        }
//                    }
//
//                    if (isContinue) {
//                        if (CommonFunction.isEmptyOrNull(dbPort)) {
//                            errorMsg = "the dbPort can not be null, please set in confile: " + confile;
//                            isContinue = false;
//                        }
//                    }
//
//                    if (isContinue) {
//                        if (CommonFunction.isEmptyOrNull(username)) {
//                            errorMsg = "the username can not be null, please set in confile: " + confile;
//                            isContinue = false;
//                        }
//                    }
//
//                    if (isContinue) {
//                        if (CommonFunction.isEmptyOrNull(password)) {
//                            errorMsg = "the password can not be null, please set in confile: " + confile;
//                            isContinue = false;
//                        }
//                    }
//
//                }
//            }
//
//            if (!isContinue) {
//                logger.error(errorMsg);
//            }
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//        return isContinue;
//    }
//
//}
