package com.dhy.bigdata.mr;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

/**
 * @Project fighting-bigdata
 * @Description 主要用途描述
 * @Author lvaolin
 * @Date 2022/1/19 上午9:26
 */
public class MyMapper implements Mapper {
    @Override
    public void map(Object o, Object o2, OutputCollector outputCollector, Reporter reporter) throws IOException {

    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void configure(JobConf jobConf) {

    }
}
