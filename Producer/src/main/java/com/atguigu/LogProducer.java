package com.atguigu;

import java.io.*;
import java.lang.reflect.Array;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class LogProducer {
//    private String caller;
//    private String callee;
//    private String build_time;
//    private String date_time;
//    private String date_time_ts;
//    private String duration;
    String caller;//主叫用户
    String callee;//被叫用户
    String build_time;//通话时长
    String date_time;//通话建立时间
    String record;//整条记录
    ArrayList<String> phoneNum_list = new ArrayList<>();
    HashMap<String, String> phoneName = new HashMap<>();
    String startTime="2019-01-01";
    String endTime="2020-01-01";
    Long date_time_ts;
    Long duration;
    OutputStreamWriter osw;
    public static void main(String[] args) {
        if (args.length <= 0) {
            System.out.println("没有参数");
            System.exit(0);
        }
        LogProducer logProducer = new LogProducer();
        try {
            logProducer.writeLog(args[0]);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    //目的：生产log文件，并写出到桌面
    //      日志文件包含内容，caller,callee,build_time,,build_time_ts,duration
    //caller与build_time 建立连接

    // 限制条件：通话时间限于2019-2020
    //
    public void writeLog(String path) throws IOException, ParseException, InterruptedException {
         osw = new OutputStreamWriter(new FileOutputStream(path));
        while (true){
            record = createConnecRecord();
            System.out.print(record);
            osw.write(record);
            osw.flush();
            Thread.sleep(1000);
        }



    }

    public  void init(){
        phoneNum_list.add("18925966618");
        phoneNum_list.add("15594967792");
        phoneNum_list.add("17827356125");
        phoneNum_list.add("14250542035");
        phoneNum_list.add("14925451524");
        phoneNum_list.add("15499660332");
        phoneNum_list.add("18762513811");
        phoneNum_list.add("18562938424");
        phoneNum_list.add("16115831249");
        phoneNum_list.add("17199493144");
        phoneNum_list.add("16776478958");
        phoneNum_list.add("17472538197");
        phoneNum_list.add("13586944338");
        phoneNum_list.add("15644963845");
        phoneNum_list.add("14172699081");
        phoneNum_list.add("13334934205");
        phoneNum_list.add("13491616573");
        phoneNum_list.add("14355426320");
        phoneNum_list.add("18738073351");
        phoneNum_list.add("13801109911");

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
    //生成建立通话的随机时间，随机时间在19~20年之间
    public String createTime() throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        long startPoint = sdf.parse(startTime).getTime();
        long endPoint = sdf.parse(endTime).getTime();
        date_time_ts=(long) (Math.random() * (endPoint - startPoint))+startPoint;
        SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return sdf2.format(new Date(date_time_ts));

    }
    //生成建立通话的随机时间，随机时间在19~20年之间但是为时间戳
    public String createTimeTS() throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        long startPoint = sdf.parse(startTime).getTime();
        long endPoint = sdf.parse(endTime).getTime();
        date_time_ts=(long) (Math.random() * (endPoint - startPoint))+startPoint;
        return  date_time_ts.toString();
    }

    public String createBulidTime(){
        DecimalFormat df = new DecimalFormat("0000");
         duration =(long)(Math.random() * 30 * 60 + 1);
         return df.format(duration);
    }
    //创建通话记录
    public  String  createConnecRecord() throws ParseException {
        init();
        int  callerIndex=(int)(Math.random()*20);
        int  calleeIndex;
        caller=phoneNum_list.get(callerIndex);
        while (true){
             calleeIndex=(int)(Math.random()*20);
            if (calleeIndex!=callerIndex)  break;
        }
        callee=phoneNum_list.get(calleeIndex);
        build_time = createBulidTime();
        date_time= createTime();
         return  caller+","+callee+","+date_time+","+build_time+"\n";
    }


}
