package com.atguigu.mr;

import com.atguigu.mapper.CommMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.MRJobConfig;

import java.io.IOException;

/**
 * 通话记录 Driver
 * <p>
 * InputFormat: 从HBase中读取数据,
 * Mapper:
 * Reducer:
 * OutFormat: 把维度数据，事实数据写入mysql
 *
 * @author zhaofanqi
 */
public class CountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 可访问hadoop：/tmp 解决无权限问题
        System.setProperty("HADOOP_USER_NAME", "atguigu");
        // 是否跨平台提交任务
        Configuration configuration = HBaseConfiguration.create();
        configuration.set(MRConfig.MAPREDUCE_APP_SUBMISSION_CROSS_PLATFORM, "true");
        // 究竟运行在本地还是在集群
        configuration.set(MRConfig.FRAMEWORK_NAME, MRConfig.YARN_FRAMEWORK_NAME);
        // jar包
        configuration.set(MRJobConfig.JAR, "H:\\hadoopProject\\ct\\analysis\\target\\analysis-1.0-SNAPSHOT-jar-with-dependencies.jar");

        // job
        Job job = Job.getInstance(configuration);
        job.setJarByClass(CountDriver.class);

        // mapper
        TableMapReduceUtil.initTableMapperJob("ct:calllog", new Scan(), CountMapper.class, CommMapper.class, Text.class, job);

        // reducer
        job.setReducerClass(CountReducer.class);

        // outputFormat
        job.setOutputFormatClass(MysqlOutputFormat.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
