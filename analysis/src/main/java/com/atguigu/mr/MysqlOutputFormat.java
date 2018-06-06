package com.atguigu.mr;

import Util.JDBCInstance;
import Util.JDBCUtils;
import com.atguigu.base.BaseMapper;
import com.atguigu.mapper.CommMapper;
import com.atguigu.mapper.ConsumerMapper;
import com.atguigu.mapper.DateMapper;
import com.atguigu.obtainId.ConsumerId;
import com.atguigu.obtainId.DateId;
import com.atguigu.reducer.CountDuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MysqlOutputFormat extends OutputFormat<CommMapper, CountDuration> {
    private FileOutputCommitter committer = null;

    @Override
    public RecordWriter<CommMapper, CountDuration> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        //重写输出方法
        Connection connection = null;
        try {
            connection = JDBCInstance.getInstance();
            connection.setAutoCommit(false);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        RecordWriter mysqlRecordWriter = new MysqlRecordWriter(connection);
        return mysqlRecordWriter;
    }

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {

    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        if (committer == null) {
            Path output = getOutputPath(context);
            committer = new FileOutputCommitter(output, context);
        }
        return committer;
    }

    public static Path getOutputPath(JobContext job) {
        String name = job.getConfiguration().get(FileOutputFormat.OUTDIR);
        return name == null ? null : new Path(name);
    }

    static class MysqlRecordWriter extends RecordWriter<BaseMapper, CountDuration> {
        private Connection connection;
        private PreparedStatement preparedStatement;
        private int batchBound = 500;//缓存sql条数边界
        private int batchSize = 0;//客户端已经缓存的条数

        private MysqlRecordWriter(Connection connection) {
            this.connection = connection;
            String sql = "INSERT INTO `tb_call` VALUES(?,?,?,?,?) ON DUPLICATE KEY UPDATE `call_sum`=? ,`call_duration_sum`=?;";
            try {
                preparedStatement = connection.prepareStatement(sql);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        /*
        key中的数据：commMapper，但是consumerMapper和DateMapper的内容作为了它的属性
         */
        @Override
        public void write(BaseMapper key, CountDuration value) throws IOException, InterruptedException {
            //查询用户id 无则插入并返回插入的id
            //查询日期id 无则插入并返回插入的id
            //将拼接的id_date_contact,id_date_dimension ,id_contact,call_sum,call_duration_sum

            CommMapper commMapper = (CommMapper) key;
            //获取第二个参数
            DateMapper dateMapper = commMapper.getDateMapper();
            int idDateDimension = DateId.getId(dateMapper);

            //获取第三个参数
            ConsumerMapper consumerMapper = commMapper.getConsumerMapper();
            ConsumerId consumerId = new ConsumerId();
            int idContact = consumerId.getId(consumerMapper);

            //获取第一个参数
            String idDateContact = idContact + "_" + idDateDimension;

            //获取第四第五个参数
            String callSum = value.getCountSum();
            String callDurationSum = value.getDurationSum();
            synchronized (this) {
                try {
                    int i = 0;
                    preparedStatement.setString(++i, idDateContact);
                    preparedStatement.setInt(++i, idDateDimension);
                    preparedStatement.setInt(++i, idContact);
                    preparedStatement.setInt(++i, Integer.valueOf(callSum));
                    preparedStatement.setInt(++i, Integer.valueOf(callDurationSum));
                    preparedStatement.setInt(++i, Integer.valueOf(callSum));
                    preparedStatement.setInt(++i, Integer.valueOf(callDurationSum));
                    //将sql缓存到客户端
                    preparedStatement.addBatch();

                    batchSize++;
                    if (batchSize > batchBound) {
                        preparedStatement.executeBatch();
                        connection.commit();
                        batchSize = 0;
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }


        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            if (preparedStatement != null) {
                try {
                    preparedStatement.executeBatch();
                    connection.commit();
                    JDBCUtils.close(connection, preparedStatement, null);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

        }
    }
}
