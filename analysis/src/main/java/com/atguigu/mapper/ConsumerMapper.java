package com.atguigu.mapper;

import com.atguigu.base.BaseMapper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ConsumerMapper extends BaseMapper {
    private String phoneNum;
    private String conName;

    @Override
    public int compareTo(BaseMapper o) {
        ConsumerMapper other = (ConsumerMapper) o;
        int result = this.phoneNum.compareTo(((ConsumerMapper) o).phoneNum);
        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(phoneNum);
        dataOutput.writeUTF(conName);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.phoneNum = dataInput.readUTF();
        this.conName = dataInput.readUTF();
    }

    public ConsumerMapper() {
    }

    public String getPhoneNum() {
        return phoneNum;
    }

    public void setPhoneNum(String phoneNum) {
        this.phoneNum = phoneNum;
    }

    public String getConName() {
        return conName;
    }

    public void setConName(String conName) {
        this.conName = conName;
    }

    @Override
    public String toString() {
        return phoneNum + "\t" + conName;
    }
}
