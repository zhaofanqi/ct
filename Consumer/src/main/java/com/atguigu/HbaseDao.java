package com.atguigu;

import MyUtils.HbaseUtil;
import constant.Constant;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class HbaseDao {
    //确定往哪张表中写数据
    private static SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static String tableName=null;
    private static String[] columnFamily=new String[5];

   /* public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }*/

    public HbaseDao(String nameSpace, String tableName, byte[][] splitkey, String... cfs ) {
        try {

           HbaseUtil.initNameSpace(nameSpace);
            HbaseUtil.createTable(tableName,cfs);
            //将table设置为静态类供使用
            this.tableName=tableName;
            //需要将传入列族全部放入静态columnFamily数组中
            columnFamily=cfs.clone();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //需要写入hbase表中的数据
    //15596505995,17519874292,2017-03-11 00:30:19,0652
    public void put(Object value) throws IOException, ParseException {
        String record = value.toString();
        String[] detailRecord = record.split(",");
        String caller = detailRecord[0];
        String callee = detailRecord[1];
        //此时获取到的时间格式为yyyy-MM-dd HH:mm:ss
        String buildTime = detailRecord[2];
        //将写入的时间变为时间戳
        String buildTime_ts = String.valueOf(sdf.parse(buildTime).getTime());

        String duration = detailRecord[3];
        //获取到rowkey
        //rowkey 格式为：recordHash+"_"+caller+"_"+buildTime+"_"+callee+"_"+duration;
        String rowKey = HbaseUtil.getRowKey(caller, callee, buildTime, duration);
        //开始写数据
        Put put = new Put(Bytes.toBytes(rowKey));
        Table table = HbaseUtil.connection.getTable(TableName.valueOf(tableName));
        put.addColumn(Bytes.toBytes(columnFamily[0]),Bytes.toBytes("call1"),Bytes.toBytes(caller));
        put.addColumn(Bytes.toBytes(columnFamily[0]),Bytes.toBytes("buildTime"),Bytes.toBytes(buildTime));
        put.addColumn(Bytes.toBytes(columnFamily[0]),Bytes.toBytes("buildTime_ts"),Bytes.toBytes(buildTime_ts));
        put.addColumn(Bytes.toBytes(columnFamily[0]),Bytes.toBytes("call2"),Bytes.toBytes(callee));
        put.addColumn(Bytes.toBytes(columnFamily[0]),Bytes.toBytes("duration"),Bytes.toBytes(duration));

        table.put(put);

    }

    public void putWithFlag(Object value) throws IOException, ParseException {
        String record = value.toString();
        String[] detailRecord = record.split(",");
        String caller = detailRecord[0];
        String callee = detailRecord[1];
        //此时获取到的时间格式为yyyy-MM-dd HH:mm:ss
        String buildTime = detailRecord[2];
        //将写入的时间变为时间戳
        String buildTime_ts = String.valueOf(sdf.parse(buildTime).getTime());

        String duration = detailRecord[3];

       // String flag=detailRecord[4];
        //获取到rowkey
        //rowkey 格式为：recordHash+"_"+caller+"_"+buildTime+"_"+callee+"_"+flag+"_"+duration;
        String rowKey = HbaseUtil.getRowKeyWithFlag(caller, callee, buildTime, duration, Constant.FLAG);
        //开始写数据
        Put put = new Put(Bytes.toBytes(rowKey));
        Table table = HbaseUtil.connection.getTable(TableName.valueOf(tableName));
        put.addColumn(Bytes.toBytes(columnFamily[0]),Bytes.toBytes("call1"),Bytes.toBytes(caller));
        put.addColumn(Bytes.toBytes(columnFamily[0]),Bytes.toBytes("buildTime"),Bytes.toBytes(buildTime));
        put.addColumn(Bytes.toBytes(columnFamily[0]),Bytes.toBytes("buildTime_ts"),Bytes.toBytes(buildTime_ts));
        put.addColumn(Bytes.toBytes(columnFamily[0]),Bytes.toBytes("call2"),Bytes.toBytes(callee));
        put.addColumn(Bytes.toBytes(columnFamily[0]),Bytes.toBytes("duration"),Bytes.toBytes(duration));
        put.addColumn(Bytes.toBytes(columnFamily[0]),Bytes.toBytes("flag"),Bytes.toBytes(duration));

        table.put(put);
    }
    /*
    为了将put方法拆开，供2种情况使用，特将put前获取的数据和时间需要put的数据抽离出来
     */
  /*  public String getOriData(Object value) throws ParseException {
        String record = value.toString();
        String[] detailRecord = record.split(",");
        String caller = detailRecord[0];
        String callee = detailRecord[1];
        //此时获取到的时间格式为yyyy-MM-dd HH:mm:ss
        String buildTime = detailRecord[2];
        //将写入的时间变为时间戳
        String buildTime_ts = String.valueOf(sdf.parse(buildTime).getTime());

        String duration = detailRecord[3];
        //获取到rowkey
        //rowkey 格式为：recordHash+"_"+caller+"_"+buildTime+"_"+callee+"_"+duration;
        String rowKey = HbaseUtil.getRowKey(caller, callee, buildTime, duration);
        return rowKey;
    }*/
}
