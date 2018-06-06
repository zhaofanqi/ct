package com.atguigu.mr;

import com.atguigu.mapper.CommMapper;
import com.atguigu.mapper.ConsumerMapper;
import com.atguigu.mapper.DateMapper;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CountMapper  extends TableMapper<CommMapper,Text>{
   // private CommMapper commMapper=new CommMapper();
    private Map<String, String> phoneName = new HashMap<>();
    private Text v=new Text();
   // private ConsumerMapper consumerMapper=new ConsumerMapper();


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        init();
    }

    private void init() {
        phoneName.put("18925966618","李雁");
        phoneName.put("15594967792","卫艺");
        phoneName.put("17827356125","仰莉");
        phoneName.put("14250542035","陶欣悦");
        phoneName.put("14925451524","施梅梅");
        phoneName.put("15499660332","金虹霖");
        phoneName.put("18762513811","魏明艳");
        phoneName.put("18562938424","华贞");
        phoneName.put("16115831249","华啟倩");
        phoneName.put("17199493144","仲采绿");
        phoneName.put("16776478958","卫丹");
        phoneName.put("17472538197","戚丽红");
        phoneName.put("13586944338","何翠柔");
        phoneName.put("15644963845","钱溶艳");
        phoneName.put("14172699081","钱琳");
        phoneName.put("13334934205","缪静欣");
        phoneName.put("13491616573","焦秋菊");
        phoneName.put("14355426320","吕访琴");
        phoneName.put("18738073351","沈丹");
        phoneName.put("13801109911","褚美丽");
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        //获取一条数据（因为rowKey包含了主要信息，所以获取rowKey就够了）
        //获取rowKey
        byte[] row = value.getRow();
        String rowKey = Bytes.toString(row);
        //切割rowkey获取需要的信息
        //recordHash+"_"+caller+"_"+buildTime+"_"+callee+"_"+flag+"_"+duration
        String[] detail = rowKey.split("_");
        String flag = detail[4];
        if("0".equals(flag)){
            return ;
        }
        String call1 = detail[1];
        String buildTime=detail[2];
        String year = buildTime.substring(0, 4);
        String month = buildTime.substring(5, 7);
        String day = buildTime.substring(8, 10);
        String call2=detail[3];
        String duration =detail[5];
        v.set(duration);

        ConsumerMapper consumerMapper=new ConsumerMapper();
        CommMapper commMapper=new CommMapper();

        //call1维度封装
        consumerMapper.setPhoneNum(call1);
        consumerMapper.setConName(phoneName.get(call1));

        DateMapper yearMapper = new DateMapper(year, "-1", "-1");
        commMapper.setConsumerMapper(consumerMapper);
        commMapper.setDateMapper(yearMapper);
        //年维度书写
        context.write(commMapper,v);

        DateMapper monthMapper = new DateMapper(year, month, "-1");
        commMapper.setDateMapper(monthMapper);
        //月维度书写
        context.write(commMapper,v);

        DateMapper dayMapper = new DateMapper(year, month,day);
        commMapper.setDateMapper(dayMapper);
        //日维度书写
        context.write(commMapper,v);


        consumerMapper.setPhoneNum(call2);
        consumerMapper.setConName(phoneName.get(call2));
        commMapper.setConsumerMapper(consumerMapper);
        commMapper.setDateMapper(yearMapper);
        //年维度书写
        context.write(commMapper,v);
        commMapper.setDateMapper(monthMapper);
        //月维度书写
        context.write(commMapper,v);
        commMapper.setDateMapper(dayMapper);
        //日维度书写
        context.write(commMapper,v);

    }
}
