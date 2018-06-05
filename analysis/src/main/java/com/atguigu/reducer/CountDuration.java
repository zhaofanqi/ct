package com.atguigu.reducer;

import com.atguigu.base.BaseValue;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CountDuration extends BaseValue {
    //某个维度通话次数总和
    private String countSum;
    //某个维度通话时间总和
    private String durationSum;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(countSum);
        dataOutput.writeUTF(durationSum);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.countSum=dataInput.readUTF();
        this.durationSum=dataInput.readUTF();
    }

    public String getCountSum() {
        return countSum;
    }

    public void setCountSum(String countSum) {
        this.countSum = countSum;
    }

    public String getDurationSum() {
        return durationSum;
    }

    public void setDurationSum(String durationSum) {
        this.durationSum = durationSum;
    }

    public CountDuration() {
    }

    @Override
    public String toString() {
        return countSum + "\t" +durationSum ;
    }
}
