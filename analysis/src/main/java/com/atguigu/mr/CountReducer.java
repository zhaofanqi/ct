package com.atguigu.mr;

import com.atguigu.mapper.CommMapper;
import com.atguigu.reducer.CountDuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CountReducer extends Reducer<CommMapper, Text, CommMapper, CountDuration> {

    private CountDuration countDuration = new CountDuration();

    @Override
    protected void reduce(CommMapper key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int countSum = 0;
        int durationSum = 0;
        for (Text value : values) {
            countSum++;
            durationSum += Integer.valueOf(value.toString());
        }
        countDuration.setCountSum(String.valueOf(countSum));
        countDuration.setDurationSum(String.valueOf(durationSum));
        context.write(key, countDuration);
    }
}
