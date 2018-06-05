package com.atguigu.mr;

import com.atguigu.mapper.CommMapper;
import com.atguigu.reducer.CountDuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CountReducer  extends Reducer<CommMapper,Text,CommMapper,CountDuration>{
    private CountDuration countDuration=new CountDuration();
    private int countSum=0;
    private int durationSum=0;

    @Override
    protected void reduce(CommMapper key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            countSum++;
            durationSum+=Integer.valueOf(value.toString());
        }
        countDuration.setCountSum(countSum+"");
        countDuration.setDurationSum(durationSum+"");
        context.write(key,countDuration);
    }
}
