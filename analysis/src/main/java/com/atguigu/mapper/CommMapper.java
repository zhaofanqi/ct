package com.atguigu.mapper;

import com.atguigu.base.BaseMapper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CommMapper extends BaseMapper {
    private ConsumerMapper consumerMapper=new ConsumerMapper();
    private DateMapper dateMapper=new DateMapper();

    @Override
    public int compareTo(BaseMapper o) {
            CommMapper other= (CommMapper) o;
        int result=this.consumerMapper.compareTo(other.consumerMapper);
        if (result == 0) {
            result=this.dateMapper.compareTo(other.dateMapper);
        }
        return  result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
            this.dateMapper.write(dataOutput);
            this.consumerMapper.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.dateMapper.readFields(dataInput);
        this.consumerMapper.readFields(dataInput);
    }

    public CommMapper() {
    }


    public ConsumerMapper getConsumerMapper() {
        return consumerMapper;
    }

    public void setConsumerMapper(ConsumerMapper consumerMapper) {
        this.consumerMapper = consumerMapper;
    }

    public DateMapper getDateMapper() {
        return dateMapper;
    }

    public void setDateMapper(DateMapper dateMapper) {
        this.dateMapper = dateMapper;
    }

    @Override
    public String toString() {
        return  consumerMapper +"\t"+ dateMapper ;
    }
}
