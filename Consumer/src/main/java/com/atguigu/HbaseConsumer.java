package com.atguigu;

import MyUtils.KafkaUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;


public class HbaseConsumer {
    public static void main(String args[]) {
        KafkaConsumer<Object, Object> kafkaConsumer = new KafkaConsumer<>(KafkaUtils.properties);
        kafkaConsumer.subscribe(Collections.singletonList(KafkaUtils.properties.getProperty("kafka.topics")));
       // new HbaseDao(tableName,columnFamily,B());
        String nameSpace="ct";
        String tableName=KafkaUtils.properties.getProperty("tableName");
       // List<String> cf = Arrays.asList("f1");
        String cf1="f1";
     //   String cf2="";
        byte[][] splitkey=new byte[10][];
        HbaseDao hbaseDao = new HbaseDao(nameSpace,tableName,splitkey, cf1,"f2" );
        while (true) {
            ConsumerRecords<Object, Object> poll = kafkaConsumer.poll(1000);
            for (ConsumerRecord<Object, Object> record : poll) {
                record.value();
                System.out.println("HbaseConsumer:"+record.value());
                try {
                    //无flag
                //   hbaseDao.put( record.value());
                    //添加flag后
                  hbaseDao.putWithFlag( record.value());
                  //  HbaseFilterUtil.gteqFilter()
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }


}
