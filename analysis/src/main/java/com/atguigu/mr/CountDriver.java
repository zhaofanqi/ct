package com.atguigu.mr;

import com.atguigu.mapper.CommMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class CountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
       /* System.setProperty("HADOOP_USER_NAME", "atguigu");

        Configuration configuration = HBaseConfiguration.create();
// 是否跨平台提交任务
        configuration.set(MRConfig.MAPREDUCE_APP_SUBMISSION_CROSS_PLATFORM, "true");
// 究竟运行在本地还是在集群
        configuration.set(MRConfig.FRAMEWORK_NAME, MRConfig.YARN_FRAMEWORK_NAME);
// jar包
        configuration.set(MRJobConfig.JAR, "H:\\hadoopProject\\ct\\analysis\\target\\analysis-1.0-SNAPSHOT-jar-with-dependencies.jar");
        //job*/
        Job job = Job.getInstance(new Configuration());
        //set jar
        job.setJarByClass(CountDriver.class);
        //set mapper
        TableMapReduceUtil.initTableMapperJob("ct:calllog",new Scan(), CountMapper.class, CommMapper.class, Text.class,job);
        //set reducer
        job.setReducerClass(CountReducer.class);

        //set selfDefinition
        job.setOutputFormatClass(MysqlOutputFormat.class);

        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);
    }
}
