package com.dhy.bigdata.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HbaseDemo2 {
 
    private static Admin admin;
 
    public static void main(String[] args) throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.zookeeper.quorum", "172.16.30.12");
        Connection connection = ConnectionFactory.createConnection(configuration);

        TableName tableName = TableName.valueOf("user_table");
        //------------DDL-建表、删表-------
        Admin admin = connection.getAdmin();
        admin.tableExists(tableName);

        //--------DML 增加删除修改 行信息----
        Table table = connection.getTable(tableName);

        Put put = new Put(("user-1").getBytes());//指定主键  rowKey
        //参数：1.列族名  2.列名  3.值
        put.addColumn("information".getBytes(), "username".getBytes(), "老吕".getBytes()) ;
        put.addColumn("information".getBytes(), "age".getBytes(), "18".getBytes()) ;
        put.addColumn("information".getBytes(), "gender".getBytes(), "男".getBytes()) ;
        put.addColumn("contact".getBytes(), "phone".getBytes(), "190000000".getBytes());
       // put.addColumn("contact".getBytes(), "email".getBytes(), "dahy@163.com".getBytes());

        table.put(put);//插入行
        //table.delete();//删除

        //--------DQL 查询 select ----

        //Get 用来构造查询条件的
        Get get = new Get("user-1".getBytes()); //主键 rowkey
        //get.addColumn("contact".getBytes(),"email".getBytes());
        Result result = table.get(get);//查询
        System.out.println(new String(result.getRow()));

        //-------DPL 事务处理 BEGIN TRANSACTION、COMMIT和ROLLBACK----

        //其它
        //connection.getBufferedMutator()

    }



    static public class User {

        private String id;
        private String username;
        private String password;
        private String gender;
        private String age;
        private String phone;
        private String email;

        public User(String id, String username, String password, String gender, String age, String phone, String email) {
            this.id = id;
            this.username = username;
            this.password = password;
            this.gender = gender;
            this.age = age;
            this.phone = phone;
            this.email = email;
        }

        public User(){

        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public String getGender() {
            return gender;
        }

        public void setGender(String gender) {
            this.gender = gender;
        }

        public String getAge() {
            return age;
        }

        public void setAge(String age) {
            this.age = age;
        }

        public String getPhone() {
            return phone;
        }

        public void setPhone(String phone) {
            this.phone = phone;
        }

        public String getEmail() {
            return email;
        }

        public void setEmail(String email) {
            this.email = email;
        }

        @Override
        public String toString() {
            return "User{" +
                    "id='" + id + '\'' +
                    ", username='" + username + '\'' +
                    ", password='" + password + '\'' +
                    ", gender='" + gender + '\'' +
                    ", age='" + age + '\'' +
                    ", phone='" + phone + '\'' +
                    ", email='" + email + '\'' +
                    '}';
        }
    }
}